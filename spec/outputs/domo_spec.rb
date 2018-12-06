# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/domo"
require "logstash/event"
require "yaml"

describe LogStash::Outputs::Domo do
  let(:user_config) do
    test_settings_file = File.dirname(File.dirname(File.dirname(__FILE__)))
    test_settings_file = File.join(test_settings_file, 'testing')
    test_settings_file = File.join(test_settings_file, 'rspec_settings.yaml')

    if File.exists?(test_settings_file)
      YAML.load_file(test_settings_file)
    else
      {}
    end
  end
  let(:event) do
    LogStash::Event.new(
      "Column1" => 456,
      "Column2" => 789,
    )
  end
  let(:lock_servers) do
    redis_servers = Array.new
    ENV.each do |k, v|
      if k.start_with? "LOCK_HOST"
        i = k.split('_')[-1].to_i

        host = v
        port = ENV.fetch("LOCK_PORT_#{i}", "6379").to_i
        db = ENV.fetch("LOCK_DB_#{i}", "0").to_i
        password = ENV.fetch("LOCK_PASSWORD_#{i}", nil)

        server = {
            "host"     => host,
            "port"     => port,
            "db"       => db,
            "password" => password,
        }

        redis_servers << server
      end
    end

    if redis_servers.length <= 0
      redis_servers = [
          {"host" => "localhost"}
      ]
    end

    redis_servers
  end
  let(:redis_client) do
    options = {
        "url" => ENV["REDIS_URL"],
    }
    sentinels = Array.new
    ENV.each do |k, v|
      if k.start_with? "REDIS_SENTINEL_HOST"
        index = k.split("_")[-1].to_i
        sentinel = {
            "host" => v,
            "port" => ENV.fetch("REDIS_SENTINEL_PORT_#{index}", 26379)
        }

        sentinels << sentinel
      end
    end

    options["sentinels"] = sentinels
    options
  end

  describe "test settings" do
    subject { user_config }

    it "should have authentication credentials" do
      expect(subject).not_to be_empty
      expect(subject).to have_key("client_id").and have_key("client_secret")
    end

    it "should have a dataset_id or stream_id" do
      expect(subject).to have_key("dataset_id").or have_key("stream_id")
    end
  end

  context "#send" do
    let(:events) do
      events = Array.new
      5.times do
        events << event
      end

      events
    end
    let(:dataset_id) { user_config.fetch("dataset_id", nil) }
    let(:stream_id) { user_config.fetch("stream_id", nil) }
    let(:config) do
      user_config.clone.merge(
          {
              "stream_id" => stream_id,
              "dataset_id" => dataset_id,
              "distributed_lock" => true,
              "lock_servers" => lock_servers,
              "redis_client" => redis_client,
          })
    end

    subject { LogStash::Outputs::Domo.new(config) }

    before(:each) do
      subject.register
    end

    # it "should have a valid config" do
    #   expect(config).to have_key("dataset_id").or have_key("stream_id")
    #   expect(config["dataset_id"] || config["stream_id"]).not_to be_nil
    # end

    it "should send the event to DOMO" do
      subject.multi_receive(events)
      subject.multi_receive([event])
    end

    after(:each) do
      subject.close
    end
  end
end
