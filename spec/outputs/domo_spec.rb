# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/domo"
require "logstash/codecs/plain"
require "logstash/codecs/csv"
require "logstash/event"

describe LogStash::Outputs::Domo do
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
      "client_id" => "",
      "client_secret" => "",
      "codec" => codec,
    }
    raise Exception.new("Fix this")
  end

  describe "#send" do
    let(:events) { [event] }
    let(:dataset_id) { "" }
    let(:stream_id) { nil }
    let(:config) { base_config.merge({"stream_id" => stream_id, "dataset_id" => dataset_id}) }

    subject { LogStash::Outputs::Domo.new(config) }

    before(:each) do
      subject.register
    end

    it "should instantiate" do
      expect(subject).to be
    end

    it "should send the event to DOMO" do
      subject.multi_receive([event])
    end
  end
end
