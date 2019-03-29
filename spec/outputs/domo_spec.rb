# encoding: utf-8
require "active_record"
require "activerecord-jdbc-adapter"
require "java"
require "logstash/devutils/rspec/spec_helper"
require "domo/queue"
require "domo/models"
require "logstash/outputs/domo"
require "logstash/event"
require "core_extensions/flatten"
require_relative "../../spec/domo_spec_helper"

RSpec.shared_examples "LogStash::Outputs::Domo" do
  context "when setting an upload timestamp", upload_timestamp: true do
    include_context "dataset bootstrap" do
      let(:test_settings) { get_test_settings }
      let(:domo_client) { get_domo_client(test_settings) }
      let(:stream_config) { bootstrap_dataset(domo_client, "_BATCH_DATE_") }
    end

    let(:config) do
      global_config.clone.merge(
          {
              "upload_timestamp_field" => "_BATCH_DATE_",
          }
      )
    end

    it "rejects upload timestamp columns not in the Dataset's schema", skip_before: true, skip_close: true do
      config["upload_timestamp_field"] = "nonexistent_field"
      expect { subject.register }.to raise_exception(LogStash::ConfigurationError)
    end

    it "automatically sets the upload timestamp" do
      subject.multi_receive(events)
      sleep(0.1) until subject.queue_processed?
      wait_for_commit(subject)

      expected_domo_data = events.map do |event|
        e = event_to_domo_hash(event)
        e["_BATCH_DATE_"] = Time.now.utc.to_datetime
        e
      end

      commit_thread = subject.instance_variable_get(:@commit_thread)
      if commit_thread&.status
        commit_thread.run if commit_thread.status == 'sleep'
        commit_thread.join
      end
      expect(dataset_field_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
    end
  end

  context "when using a partition date field", partition: true do
    include_context "dataset bootstrap" do
      let(:test_settings) { get_test_settings }
      let(:domo_client) { get_domo_client(test_settings) }
      let(:stream_config) { bootstrap_dataset(domo_client, nil, "_PARTITION_ID_") }
    end

    let(:config) do
      global_config.clone.merge(
          {
              "partition_field" => "_PARTITION_ID_",
          }
      )
    end

    it "rejects partition fields not in the Dataset's schema", skip_before: true, skip_close: true do
      config["partition_field"] = "nonexistent_field"
      expect { subject.register }.to raise_exception(LogStash::ConfigurationError)
    end

    it "automatically sets the partition field to the current date" do
      subject.multi_receive(events)
      wait_for_commit(subject)

      expected_domo_data = events.map do |event|
        e = event_to_domo_hash(event)
        e["_PARTITION_ID_"] = Time.now.utc.to_date
        e
      end

      expect(dataset_field_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
    end
  end

  context "when receiving multiple events" do
    let(:config) do
      global_config
    end
    it "sends the events to DOMO" do
      subject.multi_receive(events)

      expected_domo_data = events.map { |event| event_to_domo_hash(event) }
      expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
    end

    it "rejects mistyped events" do
      allow(subject.instance_variable_get(:@logger)).to receive(:error)

      subject.multi_receive([mistyped_event])
      expect(subject.instance_variable_get(:@logger)).to have_received(:error).with(/^.*is an invalid type for/, anything).once
    end

    it "tolerates events with null values" do
      subject.multi_receive([nil_event])
      expected_domo_data = event_to_domo_hash(nil_event)
      expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
    end

    it "handles being spammed with events", spam: true, slow: true, skip_close: true do
      spam_events = (1..200).map do |i|
        LogStash::Event.new("Event Name"      => i.to_s,
                            "Count"           => i,
                            "Event Timestamp" => LogStash::Timestamp.now,
                            "Event Date"      => Date.today.to_s,
                            "Percent"         => ((i.to_f/200)*100).round(2))
      end

      expected_domo_data = spam_events.map { |event| event_to_domo_hash(event) }

      spam_threads = Array.new
      spam_threads << Thread.new { subject.multi_receive(spam_events.slice(0..74)) }
      spam_threads << Thread.new { subject.multi_receive(spam_events.slice(75..99)) }
      spam_threads << Thread.new { subject.multi_receive(spam_events.slice(100..-1)) }
      spam_threads.each(&:join)

      commit_thread = subject.instance_variable_get(:@commit_thread)
      unless commit_thread.nil? or !commit_thread.status
        commit_thread.join
      end

      subject.close
      close_thread = subject.instance_variable_get(:@commit_thread)
      close_thread.join if close_thread&.status

      expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
    end

    context "when there is a commit delay", commit_delay: true, slow: true do
      let(:config) do
        global_config.clone.merge(
            {
                "commit_delay" => 10
            }
        )
      end
      it "honors the delay" do
        allow(subject.instance_variable_get(:@logger)).to receive(:debug)

        # subject.instance_variable_set(:@commit_delay, 10)
        queue = subject.instance_variable_get(:@queue)
        queue.set_last_commit(Time.now.utc - 1)
        subject.instance_variable_set(:@queue, queue)

        expected_domo_data = events.map { |event| event_to_domo_hash(event) }
        expected_domo_data += expected_domo_data

        subject.multi_receive(events)
        wait_for_commit(subject, true)
        expect(subject.instance_variable_get(:@logger)).to have_received(:debug).with(/The API is not ready for committing yet/, anything).once

        commit_thread = subject.instance_variable_get(:@commit_thread)
        sleep(0.1) while commit_thread&.status

        subject.multi_receive(events)
        wait_for_commit(subject, true)
        expect(subject.instance_variable_get(:@logger)).to have_received(:debug).with(/The API is not ready for committing yet/, anything).twice

        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
      end

      it "interrupts sleeping commits on close", skip_close: true do
        expected_domo_data = events.map { |event| event_to_domo_hash(event) }
        expected_domo_data += expected_domo_data

        allow(subject.instance_variable_get(:@logger)).to receive(:debug)

        # subject.instance_variable_set(:@commit_delay, 10)
        queue = subject.instance_variable_get(:@queue)
        queue.set_last_commit(Time.now.utc - 1)
        subject.instance_variable_set(:@queue, queue)

        subject.multi_receive(events)
        wait_for_commit(subject, true)
        # commit_thread = subject.instance_variable_get(:@commit_thread)
        # until commit_thread.nil? or commit_thread.status == 'sleep' or !commit_thread.status
        #   sleep(0.1)
        #   commit_thread = subject.instance_variable_get(:@commit_thread)
        # end
        expect(subject.instance_variable_get(:@logger)).to have_received(:debug).with(/The API is not ready for committing yet/, anything).once
        # commit_thread = subject.instance_variable_get(:@commit_thread)
        # sleep(0.1) while commit_thread&.status

        # subject.instance_variable_set(:@commit_delay, 10)
        queue = subject.instance_variable_get(:@queue)
        queue.set_last_commit(Time.now.utc)
        subject.instance_variable_set(:@queue, queue)

        subject.multi_receive(events)
        commit_thread = nil
        commit_thread = subject.instance_variable_get(:@commit_thread) until commit_thread
        sleep(0.1) until commit_thread.status == 'sleep'
        subject.close
        commit_thread.join

        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
      end
    end

    context "when there is an upload batch size", commit_delay: true, slow: true, upload_batch: true do
      let(:config) do
        global_config.clone.merge(
            {
                "commit_delay" => 100,
                "upload_batch_size" => 20,
            }
        )
      end

      it "honors the batch size" do
        batch_events = (1..40).map do |i|
          LogStash::Event.new("Event Name"      => i.to_s,
                              "Count"           => i,
                              "Event Timestamp" => LogStash::Timestamp.now,
                              "Event Date"      => Date.today.to_s,
                              "Percent"         => ((i.to_f/200)*100).round(2))
        end
        expected_domo_data = batch_events.map { |event| event_to_domo_hash(event) }

        queue = subject.instance_variable_get(:@queue)
        subject.multi_receive(batch_events)
        execution_id = queue.execution_id
        wait_for_commit(subject, true)

        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data.slice(0..19))).to be(true)
        sleep(0.1) until queue.execution_id != execution_id
        wait_for_commit(subject, true)
        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
      end
    end
  end

  context "with the dead letter queue enabled", dlq: true do
    let(:dlq_writer) { double('DLQ writer') }

    before(:each) do
      subject.instance_variable_set(:@dlq_writer, dlq_writer)
    end

    it "writes invalid events to the DLQ" do
      allow(subject.instance_variable_get(:@logger)).to receive(:error)

      expect(dlq_writer).to receive(:write).with(anything, /^[a-zA-Z]* is an invalid type/)
      subject.multi_receive([mistyped_event])
    end
  end
end

shared_context "dataset bootstrap" do
  let!(:test_settings) { get_test_settings }
  let!(:domo_client) { get_domo_client(test_settings) }
  let(:stream_config) { bootstrap_dataset(domo_client) }
end

shared_context "events" do
  let!(:events) do
    (1..5).map do |i|
      cur_date = Date.today.to_s
      LogStash::Event.new("Count" => i,
                          "Event Name" => "event_#{i}",
                          "Event Timestamp" => LogStash::Timestamp.now,
                          "Event Date" => cur_date,
                          "Percent" => ((i.to_f/5)*100).round(2))
    end
  end
  let!(:mistyped_event) do
    LogStash::Event.new("Count" => 1,
                        "Event Name" => "",
                        "Event Timestamp" => LogStash::Timestamp.now,
                        "Event Date" => "fz",
                        "Percent" => 2)
  end
  let!(:nil_event) do
    LogStash::Event.new("Count" => nil,
                        "Event Name" => "nil_event",
                        "Event Timestamp" => LogStash::Timestamp.now,
                        "Event Date" => nil,
                        "Percent" => nil)
  end
end

describe CoreExtensions, extensions: true do
  subject do
    LogStash::Event.new("venue_id"=>8186, "index"=>"atv", "subscription_id"=>3083,
                        "array_val"=>[0, 1],
                        "geoip"=>{"country_name"=>"United States", "dma_code"=>635, "country_code2"=>"US", "region_name"=>"Texas", "city_name"=>"Austin", "country_code3"=>"US", "latitude"=>30.2414, "postal_code"=>"78704", "region_code"=>"TX",
                                  "location"=>{"lon"=>-97.7687, "lat"=>30.2414}, "timezone"=>"America/Chicago", "continent_code"=>"NA", "longitude"=>-97.7687, "ip"=>"71.42.223.130"},
                        "client"=>"Roku", "@timestamp"=>"2018-12-27T19:01:01.000Z", "event"=>"channel.playback", "customer_type"=>"business", "date"=>1545937261, "organization_id"=>3193, "@version"=>"1", "device_id"=>5729, "ip"=>"71.42.223.130")
  end

  before(:each) { Hash.include CoreExtensions::Flatten }

  it "properly flattens complex events", :data_structure => true do
    flattened_event = subject.to_hash.flatten_with_path
    expect(flattened_event).not_to eq(subject)
    expect(flattened_event).to be_a(Hash)
    expect(flattened_event).not_to satisfy("not have sub-hashes") { |v| v.any? { |k, v| v.is_a? Hash } }
  end
end

describe Domo::Models, models: true do
  let(:database_config) do
    {
        :adapter  => "mysql2",
        :host     => "mysql",
        :username => "root",
        :password => "root",
        :database => "domo",
    }
  end
  let(:stream) do
    Domo::Models::Stream.new do |s|
      s.dataset_id = "abc"
      s.stream_id = 1
    end
  end
  let(:stream_execution) do
    Domo::Models::StreamExecution.new do |se|
      se.execution_id = 1
    end
  end
  let(:data_part) do
    Domo::Models::DataPart.new do |d|
      d.part_id = 1
    end
  end

  before(:each) do
    ActiveRecord::Base.establish_connection(database_config)
  end

  after(:each) do
    stream.delete
  end

  describe Domo::Models::Stream do
    subject do
      stream.save
      stream
    end

    it "is aware of its active executions" do
      subject.stream_executions << stream_execution
      subject.save

      new_stream = Domo::Models::Stream.new do |s|
        s.dataset_id = "cba"
        s.stream_id = 2
      end
      new_stream_execution = Domo::Models::StreamExecution.new do |se|
        se.execution_id = 10
      end
      new_stream.stream_executions << new_stream_execution
      new_stream.save

      expect(subject.active_execution).to_not eq(new_stream_execution)
      expect(new_stream.active_execution).to_not eq(stream_execution)

      expect(subject.active_execution).to eq(stream_execution)
      expect(new_stream.active_execution).to eq(new_stream_execution)
    end

    it "knows how to commit" do
      subject.stream_executions << stream_execution
      subject.save

      expect(subject.commit_ready?).to be(true)
      expect(subject.last_commit).to be(nil)
      expect(subject.active_execution).to eq(stream_execution)

      last_commit = subject.last_commit
      timestamp = Time.now.utc
      subject.commit!(timestamp)

      expect(subject.last_commit).not_to eq(last_commit)
      expect(subject.last_commit).to eq(timestamp.to_i)
      expect(Domo::Models::StreamExecution.processing(subject.id).length).to eq(0)
      expect(subject.commit_ready?(1000)).to be(false)
      expect(subject.commit_ready?(0)).to be(true)
      expect(subject.active_execution).to be(nil)
    end
  end

  it "saves the data models" do
    stream.save
    check_stream = Domo::Models::Stream.find_by(dataset_id: stream.dataset_id)
    expect(stream).to eq(check_stream)

    stream.stream_executions << stream_execution
    stream.save
    expect(stream.stream_executions.length).to eq(1)

    stream_execution = stream.stream_executions[0]
    stream_execution.data_parts << data_part
    stream_execution.save
    expect(stream_execution.data_parts.length).to eq(1)
  end
end

describe LogStash::Outputs::Domo do
  before(:each) do |example|
    subject.register unless example.metadata[:skip_before]
  end

  after(:each) do |example|
    unless example.metadata[:skip_close]
      subject.close
      wait_for_commit(subject)
    end
    if example.exception
      puts "#{dataset_id} failed for Example #{example}"
    end
    unless ENV.fetch("KEEP_FAILED_DATASETS", false)
      domo_client.dataSetClient.delete(dataset_id)
    end
  end

  describe "with distributed locking", redlock: true do
    include_context "dataset bootstrap" do
      let(:test_settings) { get_test_settings }
      let(:domo_client) { get_domo_client(test_settings) }
    end
    include_context "events"

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

    let(:global_config) do
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
                          "Percent" => ((4.to_f/5)*100).round(2))
    end

    subject do
      begin
        config.merge!(stream_config)
        described_class.new(config)
      rescue ArgumentError
        global_config.merge!(stream_config)
        described_class.new(global_config)
      end
    end

    it_should_behave_like "LogStash::Outputs::Domo"

    it "pulls events off the redis queue", redis_queue: true do
      redis_client = subject.instance_variable_get(:@redis_client)
      part_num = redis_client.incr("#{subject.part_num_key}")
      data = subject.encode_event_data(queued_event)

      queue = Domo::Queue::Redis::JobQueue.new(redis_client, dataset_id, stream_id)
      job = Domo::Queue::Job.new(queued_event, data, part_num)
      queue.add(job)
      expect(queue.size).to eq(1)

      subject.multi_receive(events)
      wait_for_commit(subject)
      new_queue = subject.instance_variable_get(:@queue)
      expect(queue.size).to eq(0)
      expect(new_queue.size).to eq(0)
      expect(new_queue.execution_id).to be(nil)

      expected_domo_data = [event_to_domo_hash(queued_event)]
      expected_domo_data += events.map { |event| event_to_domo_hash(event) }
      expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
    end

    it "fixes commit status on #register after abnormal terminations", redis_queue: true, skip_before: true do
      sentinels = redis_sentinels.map do |s|
        host, port = s.split(":")
        {
            :host => host,
            :port => port,
        }
      end
      client = Redis.new(url: ENV["REDIS_URL"], sentinels: sentinels)
      part_num_key = "#{Domo::Queue::RedisQueue::KEY_PREFIX_FORMAT}" % {:dataset_id => dataset_id}
      part_num_key = "#{part_num_key}#{Domo::Queue::RedisQueue::KEY_SUFFIXES[:PART_NUM]}"
      part_num = client.incr(part_num_key)

      data = event_to_csv(queued_event)
      queue = Domo::Queue::Redis::JobQueue.new(client, dataset_id, stream_id)
      job = Domo::Queue::Job.new(queued_event, data, part_num)
      queue.add(job)
      expect(queue.size).to eq(1)

      queue.commit_status = :running
      queue.execution_id = 1
      queue.set_last_commit(Time.now - 200)

      subject.register
      wait_for_commit(subject)
      expected_domo_data = [event_to_domo_hash(queued_event)]
      expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
    end

    it "processes events in the queue on #register", redis_queue: true, skip_before: true do
      sentinels = redis_sentinels.map do |s|
        host, port = s.split(":")
        {
            :host => host,
            :port => port,
        }
      end
      client = Redis.new(url: ENV["REDIS_URL"], sentinels: sentinels)
      part_num_key = "#{Domo::Queue::RedisQueue::KEY_PREFIX_FORMAT}" % {:dataset_id => dataset_id}
      part_num_key = "#{part_num_key}#{Domo::Queue::RedisQueue::KEY_SUFFIXES[:PART_NUM]}"
      part_num = client.incr(part_num_key)

      data = event_to_csv(queued_event)
      queue = Domo::Queue::Redis::JobQueue.new(client, dataset_id, stream_id)
      job = Domo::Queue::Job.new(queued_event, data, part_num)
      queue.add(job)
      expect(queue.size).to eq(1)

      subject.register
      expected_domo_data = [event_to_domo_hash(queued_event)]

      expect(queue.size).to eq(0)
      expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
    end

    it "processes events in the failures queue", :failure_queue => true do
      failed_event = queued_event
      redis_client = subject.instance_variable_get(:@redis_client)
      part_num = 2
      data = subject.encode_event_data(failed_event)

      failed_job = Domo::Queue::Job.new(failed_event, data, part_num, 10000)
      expect(failed_job.part_num).to eq(part_num)
      expect(failed_job.execution_id).to eq(10000)
      failed_queue = Domo::Queue::Redis::FailureQueue.new(redis_client, dataset_id, stream_id)

      failed_queue << failed_job
      expect(failed_queue.size).to eq(1)

      subject.multi_receive(events)
      expected_domo_data = [event_to_domo_hash(failed_event)]
      expected_domo_data += events.map { |event| event_to_domo_hash(event) }
      expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
    end

    it "does not wait for an empty queue to commit when there is a commit delay", skip_close: true, commit_delay: true, slow: true do
      subject.instance_variable_set(:@commit_delay, 10)

      expected_domo_data = events.map { |event| event_to_domo_hash(event) }
      expected_domo_data += expected_domo_data
      expected_domo_data += expected_domo_data

      subject.multi_receive(events)

      test_threads = ThreadGroup.new

      receive_thread = Thread.new { subject.multi_receive(events) }
      test_threads.add(receive_thread)
      sleep(0.1) until !receive_thread or receive_thread.status == 'sleep'

      close_thread = Thread.new { subject.close }
      test_threads.add(close_thread)

      subject.multi_receive(events)
      test_threads.list.each(&:join)

      expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data, should_fail: true)).to be(false)
    end
  end

  describe "without distributed locking", thread_lock: true do
    include_context "dataset bootstrap" do
      let(:test_settings) { get_test_settings }
      let(:domo_client) { get_domo_client(test_settings) }
    end
    include_context "events"

    let(:global_config) { test_settings.clone }
    let(:dataset_id) { subject.instance_variable_get(:@dataset_id) }

    subject do
      begin
        config.merge!(stream_config)
        described_class.new(config)
      rescue ArgumentError
        global_config.merge!(stream_config)
        described_class.new(global_config)
      end
    end

    it_should_behave_like "LogStash::Outputs::Domo"
  end
end
