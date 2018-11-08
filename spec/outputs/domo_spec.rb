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
      "client_id" => "875d4299-4e54-48d5-9412-a6621c2bb8c5",
      "client_secret" => "6e4f100415ca8358c1a974074092ddd5dffb9986a6b1dd272947f89137a9a70f",
      "codec" => codec,
    }
  end

  describe "#send" do
    let(:events) { [event] }
    let(:dataset_id) { "239c3ce0-f7b2-48d0-89c1-3abcfa24655c" }
    let(:config) { base_config.merge({"stream_id" => 2085, "dataset_id" => dataset_id}) }

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
