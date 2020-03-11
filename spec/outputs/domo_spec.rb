# encoding: utf-8
require "java"
require "logstash/devutils/rspec/spec_helper"
require "domo/queue"
require "logstash/outputs/domo"
require "logstash/event"
require "core_extensions/flatten"
require "rake"
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
      sleep(0.1) until subject.get_queue.processed?
      wait_for_commit(subject)

      expected_domo_data = events.map do |event|
        e = event_to_domo_hash(event)
        e["_BATCH_DATE_"] = Time.now.utc.to_datetime
        e
      end

      commit_task = subject.instance_variable_get(:@commit_task)
      if commit_task&.pending? or commit_task&.unscheduled?
        commit_task.reschedule(0) if commit_task.pending?
        sleep(0.1) while commit_task&.processing? or commit_task&.pending?
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

  context "when receiving multiple events", multi: true do
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

    # it "unblocks commits that have been stuck for 1 hour" do
    #   expected_domo_data = events.map { |event| event_to_domo_hash(event) }
    #
    #   queue = subject.get_queue
    #   queue.execution_id = 1
    #   queue.commit_status = :running
    #   expect(queue.commit_start_time.round(0) <= Time.now.utc.round(0))
    #
    #   t = Thread.new { subject.multi_receive(events) }
    #   queue.set_commit_start_time(queue.commit_start_time - 3599)
    #   t.join if t.status
    #
    #   wait_for_commit(subject)
    #   expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
    # end

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
      spam_threads << Thread.new { subject.multi_receive(spam_events.slice(100..-1)) }
      spam_threads << Thread.new { subject.multi_receive(spam_events.slice(75..99)) }
      spam_threads.each(&:join)

      wait_for_commit(subject, true)
      sleep(0.5)

      subject.multi_receive([LogStash::SHUTDOWN])
      wait_for_shutdown(subject)

      sleep(2) # Let's sleep yet AGAIN because there seems to be a lag in data making it to Domo's Datset Export API
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

      it "honors the delay", slow: true, skip_close: true do
        allow(subject.instance_variable_get(:@logger)).to receive(:info)

        queue = subject.get_queue
        queue.set_last_commit(Time.now.utc - 1)
        subject.instance_variable_set(:@queue, queue)

        expected_domo_data = events.map { |event| event_to_domo_hash(event) }
        expected_domo_data += expected_domo_data

        subject.multi_receive(events)
        sleep(0.1) until queue.commit_scheduled?

        expect(subject.instance_variable_get(:@logger)).to have_received(:info).with(/The API is not ready for committing yet/, anything).once
        sleep(0.1) until queue.all_empty?
        sleep(0.1) until queue.commit_unscheduled? and queue.execution_id.nil?

        subject.multi_receive(events)
        sleep(0.1) until queue.commit_scheduled?
        expect(subject.instance_variable_get(:@logger)).to have_received(:info).with(/The API is not ready for committing yet/, anything).twice

        subject.multi_receive([LogStash::SHUTDOWN])
        sleep(0.1) until queue.all_empty?
        sleep(0.1) until queue.commit_unscheduled? and queue.execution_id.nil?
        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
      end

      it "does not commit on #close unless ready", skip_close: true do
        expected_domo_data = events.map { |e| event_to_domo_hash(e) }
        expected_domo_data += expected_domo_data
        allow(subject.instance_variable_get(:@logger)).to receive(:info)

        queue = subject.get_queue

        subject.multi_receive(events)
        subject.multi_receive(events)
        expect(subject.instance_variable_get(:@logger)).to have_received(:info).with(/The API is not ready for committing yet/, anything).once

        last_commit = queue.last_commit

        subject.close

        expect(queue.length > 0)
        expect(queue.last_commit).to eq(last_commit)

        queue.last_commit = last_commit - 20
        subject.close

        wait_for_commit(subject, true)
        expect(queue.length).to eq(0)
        expect(queue.execution_id).to be_nil
        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
      end
    end

    context "when there is a maximum upload batch size", commit_delay: true, slow: true, upload_batch: true do
      let(:config) do
        global_config.clone.merge(
            {
                "commit_delay" => 10,
                "upload_max_batch_size" => 20,
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

        queue = subject.get_queue
        queue.last_commit = Time.now.utc
        subject.multi_receive(batch_events.slice(0..20))
        expect(queue.data_parts.length).to eq(2)
        subject.multi_receive(batch_events.slice(21..-1))

        wait_for_commit(subject, true)
        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
      end
    end

    context "when there is a minimum upload batch size", commit_delay: true, slow: true, upload_batch: true do
      let(:config) do
        global_config.clone.merge(
            {
                "commit_delay" => 10,
                "upload_min_batch_size" => 50,
            }
        )
      end

      it "waits until the batch size is reached" do
        batch_events = (1..100).map do |i|
          LogStash::Event.new("Event Name"      => i.to_s,
                              "Count"           => i,
                              "Event Timestamp" => LogStash::Timestamp.now,
                              "Event Date"      => Date.today.to_s,
                              "Percent"         => ((i.to_f/200)*100).round(2))
        end
        expected_domo_data = batch_events.map { |event| event_to_domo_hash(event) }

        queue = subject.get_queue
        queue.last_commit = Time.now.utc - config["commit_delay"]
        subject.multi_receive(batch_events.slice(0..24))
        expect(queue.length).to eq(0)
        expect(queue.pending_jobs.length).to eq(25)

        queue.last_commit = Time.now.utc
        subject.multi_receive(batch_events.slice(25..49))
        expect(queue.pending_jobs.length).to eq(0)

        subject.multi_receive(batch_events.slice(50..74))
        expect(queue.pending_jobs.length).to eq(25)

        queue.last_commit = Time.now.utc
        subject.multi_receive(batch_events.slice(75..-1))

        wait_for_commit(subject, true)
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
        spam_threads << Thread.new { subject.multi_receive(spam_events.slice(100..-1)) }
        spam_threads << Thread.new { subject.multi_receive(spam_events.slice(75..99)) }
        spam_threads.each(&:join)

        wait_for_commit(subject, true)
        sleep(0.5)

        subject.multi_receive([LogStash::SHUTDOWN])
        wait_for_shutdown(subject)

        sleep(2) # Let's sleep yet AGAIN because there seems to be a lag in data making it to Domo's Datset Export API
        expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
      end

      it "processes the pending queue when Logstash stops", skip_close: true, upload_batch: true do
        batch_events = (1..75).map do |i|
          LogStash::Event.new("Event Name"      => i.to_s,
                              "Count"           => i,
                              "Event Timestamp" => LogStash::Timestamp.now,
                              "Event Date"      => Date.today.to_s,
                              "Percent"         => ((i.to_f/200)*100).round(2))
        end
        expected_domo_data = batch_events.map { |event| event_to_domo_hash(event) }

        queue = subject.get_queue
        subject.multi_receive(batch_events.slice(0..49))
        expect(queue.length).to eq(0)

        queue.last_commit = Time.now.utc
        subject.multi_receive(batch_events.slice(50..-1))
        expect(queue.pending_jobs.length).to eq(25)

        subject.multi_receive([LogStash::SHUTDOWN])
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

describe "rake tasks", rake: true do
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

  let!(:tasks_path) do
    File.expand_path(File.join(File.dirname(File.dirname(File.dirname(__FILE__ ))), "rakelib"))
  end

  let(:redis_client) do
    redis_client = {:url => ENV["REDIS_URL"]}
    redis_client[:sentinels] = ENV.select { |k, v| k.start_with? "REDIS_SENTINEL_HOST"}.map do |k, v|
      index = k.split("_")[-1].to_i
      port = ENV.fetch("REDIS_SENTINEL_PORT_#{index}", 26379)

      {
          :host => v,
          :port => port,
      }
    end

    if redis_client[:sentinels].length <= 0
      redis_client = redis_client.reject { |k, v| k == :sentinels }
    end

    Redis.new(redis_client)
  end

  let(:jobs) do
    events = (1..10).map do |i|
      {
          "Event Name"      => i.to_s,
          "Count"           => i,
          "Event Timestamp" => Time.now.utc.to_datetime,
          "Event Date"      => Time.now.utc.to_date,
          "Percent"         => ((i.to_f/10)*100).round(2),
          "_BATCH_DATE_"    => Time.now.utc.to_date
      }
    end

    csv_encode_opts = {
        :headers => events[0].keys,
        :write_headers => false,
        :return_headers => false,
    }
    events.map do |e|
      csv_data = CSV.generate(String.new, csv_encode_opts) do |csv_obj|
        data = e.sort_by { |k, _| events[0].key(k) }.to_h
        csv_obj << data.values
      end
      csv_data = [csv_data.strip]
      Domo::Queue::Job.new(csv_data)
    end
  end

  let(:old_dataset) { stream_config }
  let(:new_dataset) { bootstrap_dataset(domo_client, "_BATCH_DATE_") }
  let(:lib_root) { File.expand_path(File.dirname(File.dirname(File.dirname(__FILE__ )))) }
  let(:rake) { Rake::Application.new }
  subject { Rake::Task[task_name] }

  before(:each) do
    rake_filename = task_name.split(':').last
    loaded_files = $".reject {|file| file == File.join(tasks_path, "#{rake_filename}.rake").to_s }
    rake.rake_require(rake_filename, [tasks_path], loaded_files)
  end

  context "when the task is domo:migrate_queue" do
    let(:task_name) { "domo:migrate_queue" }
    let!(:old_queue) { Domo::Queue::Redis::JobQueue.active_queue(redis_client, old_dataset['dataset_id'], old_dataset['stream_id'], 'main') }
    let!(:new_queue) { Domo::Queue::Redis::JobQueue.active_queue(redis_client, new_dataset['dataset_id'], new_dataset['stream_id'], 'main') }
    let!(:task_args) { [old_dataset["dataset_id"], old_dataset["stream_id"], new_dataset["dataset_id"], new_dataset["stream_id"], true] }

    let(:plugin_lock_hosts) do
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

    let(:plugin_redis_client) do
      {
          "url"  => ENV["REDIS_URL"],
      }
    end

    let(:plugin_redis_sentinels) do
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
              "lock_hosts"       => plugin_lock_hosts,
              "redis_client"     => plugin_redis_client,
              "redis_sentinels"  => plugin_redis_sentinels,
          }
      )
    end

    let(:old_queue_plugin) do
      begin
        config.merge!(stream_config)
        LogStash::Outputs::Domo.new(config)
      rescue ArgumentError
        global_config.merge!(stream_config)
        LogStash::Outputs::Domo.new(global_config)
      end
    end

    let(:new_queue_plugin) do
      begin
        config.merge!(stream_config)
        config["dataset_id"] = new_queue.instance_variable_get(:@dataset_id)
        config["stream_id"] = new_queue.instance_variable_get(:@stream_id)
        LogStash::Outputs::Domo.new(config)
      rescue ArgumentError
        global_config.merge!(stream_config)
        LogStash::Outputs::Domo.new(global_config)
      end
    end

    before(:each) do
      # Load the queue up with our jobs
      jobs.each do |job|
        old_queue << job
      end
    end

    after(:each) do |example|
      # Make sure the queues are clear
      old_queue.clear
      new_queue.clear
      # Re-enable the rake task for subsequent tests
      subject.reenable
    end

    it "moves everything from the old queue to the new queue" do
      expect(old_queue.length).to eq(10)
      expect(new_queue.length).to eq(0)

      subject.invoke(*task_args)
      expect(old_queue.length).to eq(0)
      expect(new_queue.length).to eq(10)
    end

    it "moves failed and pending jobs to the new queue" do
      old_queue.failures << jobs[0]

      pending_data = old_queue.pop.data
      old_queue.pending_jobs << [pending_data, pending_data]

      expect(old_queue.length).to eq(9)
      expect(old_queue.failures.length).to eq(1)
      expect(old_queue.pending_jobs.length).to eq(2)

      subject.invoke(*task_args)

      expect(old_queue.length).to eq(0)
      expect(old_queue.pending_jobs.length).to eq(0)
      expect(old_queue.all_empty?).to be(true)

      expect(new_queue.length).to eq(11)
      data_rows = Array.new
      new_queue.each_with_index do |job, i|
        expect(job.data.length).to be >= 1
        data_rows += job.data
      end

      expect(data_rows.length).to eq(12)
    end

    it "creates jobs that the plugin can actually process", foo: true do
      expected_domo_data = jobs.map do |job|
        job_data_to_hash(job, "_BATCH_DATE_")
      end

      expect(old_queue.length).to eq(10)
      expect(new_queue.length).to eq(0)

      subject.invoke(*task_args)
      expect(old_queue.length).to eq(0)
      expect(new_queue.length).to eq(10)

      new_queue_plugin.register
      wait_for_commit(new_queue_plugin)
      new_queue_plugin.close

      dataset_id = new_queue_plugin.instance_variable_get(:@dataset_id)
      expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
    end
  end

  after(:each) do |example|
    if example.exception
      puts "Example #{example} failed."
    end
    unless ENV.fetch("KEEP_FAILED_DATASETS", false)
      domo_client.dataSetClient.delete(old_dataset["dataset_id"])
      domo_client.dataSetClient.delete(new_dataset["dataset_id"])
    end
  end
end

describe "configuration parser", config_test: true do
  let(:config) do
    {
        'client_id'        => 'whatever',
        'client_secret'    => 'whatever',
        'distributed_lock' => true,
        'stream_id'        => 0,
        'dataset_id'       => 'whatever',
        'lock_hosts'       => %w[
            redis://redis1:6379
            redis://redis2:6379
            rediss://:password@127.0.0.1:6379/0
        ],
        'redis_client'     => {
            'url'        => 'rediss://:password@127.0.0.1:6379/1',
            'ssl_params' => {'verify_mode' => 'VERIFY_NONE'}
        }
    }
  end
  subject { LogStash::Outputs::Domo.new(config) }

  it "should parse the redis configurations" do
    redis_client = subject.send(:recursive_symbolize, config['redis_client'])
    expect(redis_client).to be_a(Hash)
    redis_client.keys.each { |k| expect(k).to be_a(Symbol) }
    expect(redis_client).to have_key(:url)
    expect(redis_client).to have_key(:ssl_params)
    expect(redis_client[:ssl_params]).to have_key(:verify_mode)
    expect(redis_client[:ssl_params][:verify_mode]).to eq(OpenSSL::SSL::VERIFY_NONE)
    expect(Redis.new(redis_client)).to be_a(Redis)

    lock_hosts = subject.send(:lock_hosts_tls_opts, config['lock_hosts'])
    lock_hosts.each { |h| expect(h).to be_a(Redis) }
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
      data = subject.encode_event_data(queued_event)

      queue = subject.get_queue
      job = Domo::Queue::Job.new([data])
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

      queue = Domo::Queue::Redis::JobQueue.new(client, dataset_id, stream_id, 1)
      data = event_to_csv(queued_event)
      job = Domo::Queue::Job.new([data])
      queue.add(job)
      expect(queue.size).to eq(1)

      queue.commit_status = :running
      queue.last_commit = Time.now - 200

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
      data = event_to_csv(queued_event)
      queue = Domo::Queue::Redis::JobQueue.new(client, dataset_id, stream_id)
      job = Domo::Queue::Job.new([data])
      queue.add(job)
      expect(queue.size).to eq(1)

      subject.register
      expected_domo_data = [event_to_domo_hash(queued_event)]

      expect(queue.size).to eq(0)
      expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data)).to be(true)
    end

    it "processes events in the failures queue", :failure_queue => true, slow: true do
      failed_event = queued_event
      data = subject.encode_event_data(failed_event)
      redis_client = subject.instance_variable_get(:@redis_client)

      data_part = Domo::Queue::RedisDataPart.new(1, 12131, :failed)
      failed_job = Domo::Queue::Job.new([data], 0, data_part)
      failures = Domo::Queue::Redis::FailureQueue.new(redis_client, dataset_id, stream_id)
      failures << failed_job
      expect(failures.size).to eq(1)

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
      subject.multi_receive(events)
      test_threads = ThreadGroup.new

      # receive_thread = Thread.new { subject.multi_receive(events) }
      # test_threads.add(receive_thread)
      # sleep(0.1) until !receive_thread or receive_thread.status == 'sleep'

      close_thread = Thread.new { subject.close }
      test_threads.add(close_thread)

      subject.multi_receive(events)
      test_threads.list.each(&:join)

      expect(dataset_data_match?(domo_client, dataset_id, expected_domo_data, should_fail: true)).to be(false)
    end
  end

  # TODO: Update these tests
  # describe "without distributed locking", thread_lock: true do
  #   include_context "dataset bootstrap" do
  #     let(:test_settings) { get_test_settings }
  #     let(:domo_client) { get_domo_client(test_settings) }
  #   end
  #   include_context "events"
  #
  #   let(:global_config) { test_settings.clone }
  #   let(:dataset_id) { subject.instance_variable_get(:@dataset_id) }
  #
  #   subject do
  #     begin
  #       config.merge!(stream_config)
  #       described_class.new(config)
  #     rescue ArgumentError
  #       global_config.merge!(stream_config)
  #       described_class.new(global_config)
  #     end
  #   end
  #
  #   it_should_behave_like "LogStash::Outputs::Domo"
  # end
end
