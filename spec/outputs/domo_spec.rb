# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/domo"
require "logstash/codecs/plain"
require "logstash/codecs/csv"
require "logstash/event"
require "yaml"

describe LogStash::Outputs::Domo do
  let(:user_config) do
    test_settings_file = File.dirname(File.dirname(__FILE__))
    test_settings_file = File.join(test_settings_file, 'test_settings.yaml')

    if File.exists?(test_settings_file)
      YAML.load_file(test_settings_file)
    else
      {}
    end
  end
  let(:codec) do
    LogStash::Plugin.lookup("codec", "csv").new("columns" => ["Column1", "Column2"])
  end
  let(:event) do
    LogStash::Event.new(
      "Column1" => 123,
      "Column2" => 456,
    )
  end
  let(:base_config) do
    {
      "codec" => codec,
    }
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
      base_config.merge(
          {
              "client_id" => user_config["client_id"],
              "client_secret" => user_config["client_secret"],
              "stream_id" => stream_id,
              "dataset_id" => dataset_id
          })
    end

    subject { LogStash::Outputs::Domo.new(config) }

    before(:each) do
      subject.register
    end

    it "should have a valid config" do
      expect(config).to have_key("dataset_id").or have_key("stream_id")
      expect(config["dataset_id"] || config["stream_id"]).not_to be_nil
    end

    it "should send the event to DOMO" do
      subject.multi_receive(events)
      subject.receive(event)
    end

    after(:each) do
      subject.close
    end
  end
end
