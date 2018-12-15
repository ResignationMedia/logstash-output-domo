# encoding: utf-8
require "java"
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/domo"
require "logstash/event"
require_relative "../../spec/domo_spec_helper"

describe LogStash::Outputs::Domo do
  let(:test_settings) { get_test_settings }

  before(:each) do
    subject.register
  end

  after(:each) do
    subject.close
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

      subject { described_class.new(config) }

      it "should send the event to DOMO" do
        subject.multi_receive(events)
      end

      it "should reject mistyped events" do
        allow(subject.instance_variable_get(:@logger)).to receive(:error)

        subject.multi_receive([mistyped_event])
        expect(subject.instance_variable_get(:@logger)).to have_received(:error).with(/Invalid data type/, anything).once
      end
    end

    context "without distributed locking" do
      let(:config) { test_settings.clone }
      subject { described_class.new(config) }

      it "should send the event to DOMO" do
        subject.multi_receive(events)
      end

      it "should reject mistyped events" do
        allow(subject.instance_variable_get(:@logger)).to receive(:error)

        subject.multi_receive([mistyped_event])
        expect(subject.instance_variable_get(:@logger)).to have_received(:error).with(/Invalid data type/, anything).once
      end
    end
  end
end
