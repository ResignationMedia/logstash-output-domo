# encoding: utf-8
require "java"
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/domo"
require "logstash/event"
require "core_extensions/flatten"
require_relative "../../spec/domo_spec_helper"

RSpec.shared_examples "LogStash::Outputs::Domo" do
  it "should send the event to DOMO" do
    subject.multi_receive(events)

    expected_domo_data = events.map { |event| event_to_csv(event) }
    expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
  end

  it "should reject mistyped events" do
    allow(subject.instance_variable_get(:@logger)).to receive(:error)

    subject.multi_receive([mistyped_event])
    expect(subject.instance_variable_get(:@logger)).to have_received(:error).with(/^.*is an invalid type for/, anything).once
  end

  it "should tolerate events with null values" do
    subject.multi_receive([nil_event])
    expected_domo_data = event_to_csv(nil_event)
    expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
  end
end

shared_context "dataset bootstrap" do
  let!(:test_settings) { get_test_settings }
  let!(:domo_client) { get_domo_client(test_settings) }
  let!(:stream_config) { bootstrap_dataset(domo_client) }
end

describe CoreExtensions do
  subject do
    LogStash::Event.new("venue_id"=>8186, "index"=>"atv", "subscription_id"=>3083,
                        "geoip"=>{"country_name"=>"United States", "dma_code"=>635, "country_code2"=>"US", "region_name"=>"Texas", "city_name"=>"Austin", "country_code3"=>"US", "latitude"=>30.2414, "postal_code"=>"78704", "region_code"=>"TX",
                                  "location"=>{"lon"=>-97.7687, "lat"=>30.2414}, "timezone"=>"America/Chicago", "continent_code"=>"NA", "longitude"=>-97.7687, "ip"=>"71.42.223.130"},
                        "client"=>"Roku", "@timestamp"=>"2018-12-27T19:01:01.000Z", "event"=>"channel.playback", "customer_type"=>"business", "date"=>1545937261, "organization_id"=>3193, "@version"=>"1", "device_id"=>5729, "ip"=>"71.42.223.130")
  end

  before(:each) { Hash.include CoreExtensions::Flatten }

  it "should properly flatten complex events", :data_structure => true do
    flattened_event = subject.to_hash.flatten_with_path
    expect(flattened_event).not_to eq(subject)
    expect(flattened_event).to be_a(Hash)
    expect(flattened_event).to satisfy("not have sub-hashes") { |v| v.each {|k, v| if v.is_a? Hash then false; break end }}
  end
end

describe LogStash::Outputs::Domo do
  before(:each) do
    subject.register
  end

  after(:each) do
    dataset_id = subject.instance_variable_get(:@dataset_id)
    subject.close
    domo_client.dataSetClient.delete(dataset_id)
  end

  let(:lock_hosts) do
    redis_servers = Array.new
    ENV.each do |k, v|
      if k.start_with? "LOCK_HOST"
        redis_servers << v
      end
    end

    if redis_servers.length <= 0
      redis_servers = [
          "redis://localhost:6379"
      ]
    end

    redis_servers
  end

  let(:redis_client) do
    {
        "url"  => ENV["REDIS_URL"],
    }
  end

  let(:redis_sentinels) do
    sentinels = Array.new
    ENV.each do |k, v|
      if k.start_with? "REDIS_SENTINEL_HOST"
        index = k.split("_")[-1].to_i
        port = ENV.fetch("REDIS_SENTINEL_PORT_#{index}", 26379)

        sentinel = "#{v}:#{port}"
        sentinels << sentinel
      end
    end

    sentinels
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

    # context "with DLQ enabled" do
    #   include_context "dataset bootstrap" do
    #     let(:test_settings) { get_test_settings }
    #     let(:domo_client) { get_domo_client(test_settings) }
    #   end
    #
    #   let(:config) { test_settings.clone }
    #   let(:dlq_writer) { double('DLQ writer') }
    #
    #   # before { subject.instance_variable_set('@dlq_writer', dlq_writer) }
    #   subject do
    #     config.merge!(stream_config)
    #     plugin = described_class.new(config)
    #     plugin.instance_variable_set(:@dlq_writer, dlq_writer)
    #     plugin
    #   end
    #
    #   it "should write invalid events to the DLQ" do
    #     allow(subject.instance_variable_get(:@logger)).to receive(:error)
    #
    #     expect(dlq_writer).to receive(:write).with(anything, /Invalid data type/)
    #     subject.multi_receive([mistyped_event])
    #     expect(subject.instance_variable_get(:@logger)).to have_received(:error).with(/Invalid data type/, anything).once
    #   end
    # end

    context "with distributed locking" do
      include_context "dataset bootstrap" do
        let(:test_settings) { get_test_settings }
        let(:domo_client) { get_domo_client(test_settings) }
      end

      let(:config) do
        test_settings.clone.merge(
            {
                "distributed_lock" => true,
                "lock_hosts"       => lock_hosts,
                "redis_client"     => redis_client,
                "redis_sentinels"  => redis_sentinels,
            }
        )
      end
      let(:dataset_id) { subject.instance_variable_get(:@dataset_id) }
      let(:stream_id) { subject.instance_variable_get(:@stream_id) }
      let(:queued_event) do
        LogStash::Event.new("Count" => 4,
                            "Event Name" => "queued_event",
                            "Event Timestamp" => LogStash::Timestamp.now,
                            "Event Date" => (Date.today - 1).to_s,
                            "Percent" => (4.to_f/5)*100)
      end

      subject do
        config.merge!(stream_config)
        described_class.new(config)
      end

      it_should_behave_like "LogStash::Outputs::Domo"

      it "should pull events off the redis queue" do
        redis_client = subject.instance_variable_get(:@redis_client)
        part_num = redis_client.incr("#{subject.part_num_key}")
        data = subject.encode_event_data(queued_event)

        queue = Domo::Queue.new(redis_client, dataset_id, stream_id)
        job = Domo::Job.new(queued_event, data, part_num)
        queue.add(job)
        expect(queue.size).to eq(1)

        subject.multi_receive(events)
        new_queue = subject.instance_variable_get(:@queue)
        expect(queue.size).to eq(0)
        expect(new_queue.size).to eq(0)
        expect(new_queue.execution_id).to be(nil)

        expected_domo_data = [event_to_csv(queued_event)]
        expected_domo_data += events.map { |event| event_to_csv(event) }
        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
      end
    end

    context "without distributed locking" do
      include_context "dataset bootstrap" do
        let(:test_settings) { get_test_settings }
        let(:domo_client) { get_domo_client(test_settings) }
      end

      let(:config) { test_settings.clone }
      let(:dataset_id) { subject.instance_variable_get(:@dataset_id) }

      subject do
        config.merge!(stream_config)
        described_class.new(config)
      end

      it_should_behave_like "LogStash::Outputs::Domo"
    end
  end
end
