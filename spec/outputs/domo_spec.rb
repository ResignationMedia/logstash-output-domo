# encoding: utf-8
require "java"
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/domo"
require "logstash/event"
require_relative "../../spec/domo_spec_helper"

describe LogStash::Outputs::Domo do
  let(:test_settings) { get_test_settings }
  let!(:domo_client) do
    client_config = Java::ComDomoSdkRequest::Config.with
                        .clientId(test_settings["client_id"])
                        .clientSecret(test_settings["client_secret"])
                        .apiHost("api.domo.com")
                        .useHttps(true)
                        .scope(Java::ComDomoSdkRequest::Scope::DATA)
                        .build()
    DomoClient.create(client_config)
  end
  let!(:stream_config) { bootstrap_dataset(domo_client) }

  before(:each) do
    subject.register
  end

  after(:each) do
    dataset_id = subject.instance_variable_get(:@dataset_id)
    subject.close
    domo_client.dataSetClient.delete(dataset_id)
  end

  let(:lock_servers) do
    redis_servers = Array.new
    ENV.each do |k, v|
      if k.start_with? "LOCK_HOST"
        i = k.split("_")[-1].to_i

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

  describe "#multi_receive" do
    let(:events) do
      (1..5).map do |i|
        cur_date = Date.today.to_s
        LogStash::Event.new("Count" => i,
                            "Event Name" => "event_#{i}",
                            "Event Timestamp" => LogStash::Timestamp.now,
                            "Event Date" => cur_date,
                            "Percent" => (i.to_f/5)*100)
      end
    end
    let(:mistyped_event) do
      LogStash::Event.new("Count" => 1,
                          "Event Name" => "",
                          "Event Timestamp" => LogStash::Timestamp.now,
                          "Event Date" => "fz",
                          "Percent" => 2)
    end
    let (:nil_event) do
      LogStash::Event.new("Count" => nil,
                          "Event Name" => "nil_event",
                          "Event Timestamp" => LogStash::Timestamp.now,
                          "Event Date" => nil,
                          "Percent" => nil)
    end

    context "with distributed locking" do
      let(:config) do
        test_settings.clone.merge(
            {
                "distributed_lock" => true,
                "lock_servers"     => lock_servers,
                "redis_client"     => redis_client,
            }
        )
      end
      let(:dataset_id) { subject.instance_variable_get(:@dataset_id) }

      subject do
        config.merge!(stream_config)
        described_class.new(config)
      end

      it "should send the event to DOMO" do
        subject.multi_receive(events)

        expected_domo_data = events.map { |event| event_to_csv(event) }
        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
      end

      it "should reject mistyped events" do
        allow(subject.instance_variable_get(:@logger)).to receive(:error)

        subject.multi_receive([mistyped_event])
        expect(subject.instance_variable_get(:@logger)).to have_received(:error).with(/Invalid data type/, anything).once
      end

      it "should tolerate events with null values" do
        subject.multi_receive([nil_event])
        expected_domo_data = event_to_csv(nil_event)
        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
      end
    end

    context "without distributed locking" do
      let(:config) { test_settings.clone }
      let(:dataset_id) { subject.instance_variable_get(:@dataset_id) }

      subject do
        config.merge!(stream_config)
        described_class.new(config)
      end

      it "should send the event to DOMO" do
        subject.multi_receive(events)
        expected_domo_data = events.map { |event| event_to_csv(event) }
        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
      end

      it "should reject mistyped events" do
        allow(subject.instance_variable_get(:@logger)).to receive(:error)

        subject.multi_receive([mistyped_event])
        expect(subject.instance_variable_get(:@logger)).to have_received(:error).with(/Invalid data type/, anything).once
      end

      it "should tolerate events with null values" do
        subject.multi_receive([nil_event])
        expected_domo_data = event_to_csv(nil_event)
        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
      end
    end
  end
end
