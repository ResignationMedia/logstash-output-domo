# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "core_extensions/enumerable/flatten"
require "core_extensions/concurrent/hash"
require "concurrent"
require "csv"
require "java"
require "redis"
require "redlock"
require "thread"
require "domo/client"
require "domo/queue"
require "logstash-output-domo_jars.rb"

# Add a method to Enumerable to flatten complex data structures into CSV columns
Hash.include CoreExtensions::Enumerable::Flatten
using CoreExtensions

java_import "com.domo.sdk.streams.model.Stream"
java_import "java.util.ArrayList"
java_import "java.util.concurrent.atomic.AtomicReference"

# Write events to a DOMO Streams Dataset.
#
# Requires DOMO OAuth credentials.
# https://developer.domo.com/docs/authentication/overview-4
#
# Additionally, the DOMO Streams API requires that the Dataset and Stream be made via the API.
# Streams cannot be added to existing Datasets.
# https://developer.domo.com/docs/stream/overview
class LogStash::Outputs::Domo < LogStash::Outputs::Base
  config_name "domo"

  concurrency :shared

  # OAuth ClientID
  config :client_id, :validate => :string, :required => :true

  # OAuth Client Secret
  config :client_secret, :validate => :string, :required => :true

  # The DOMO StreamID. If unknown, use the dataset_id parameter instead.
  # This parameter is preferred for performance reasons.
  config :stream_id, :validate => :number

  # The DOMO DataSetID. 
  # Will be used to lookup or create a Stream if the stream_id parameter is not specified.
  config :dataset_id, :validate => :string, :required => :true

  # The hostname for API requests
  config :api_host, :validate => :string, :default => "api.domo.com"

  # Use TLS for API requests
  config :api_ssl, :validate => :boolean, :default => true

  # Retry failed requests. Enabled by default. It would be a pretty terrible idea to disable this. 
  config :retry_failures, :validate => :boolean, :default => true

  # The delay (seconds) on retrying failed requests
  config :retry_delay, :validate => :number, :default => 2.0

  # Ensure that Event fields have the same data type as their corresponding columns in Domo.
  config :type_check, :validate => :boolean, :default => true

  # Use a distributed lock. Necessary when running the plugin against the same Dataset on multiple Logstash servers.
  config :distributed_lock, :validate => :boolean, :default => false

  # The timeout on holding locks in milliseconds.
  # It is not recommended to hold the lock for less than 3,000ms due to lag on the DOMO API.
  config :lock_timeout, :validate => :number, :default => 3000

  # The delay (ms) on retrying acquiring the lock.
  # The default matches the default for the distributed locking library (redlock)
  config :lock_retry_delay, :validate => :number, :default => 200

  # An array of redis hosts (using the redis:// URI syntax) to connect to for the distributed lock
  #
  # *REDIS SENTINEL IS NOT SUPPORTED*
  # Don't like it? Ask Elastic to make Logstash support arrays of hashes in config files.
  #
  # An example array showcasing the various redis URI options can be found below:
  # [source,ruby]
  # ----------------------------------
  # lock_hosts => [
  #   "redis://127.0.0.1:6379"
  #   "redis://:password@host2:6379"
  #   "redis://host3:6379/0"
  #   "unix+redis://some/socket/path?db=0&password=password"
  # ]
  # -----------------------
  config :lock_hosts, :validate => :array

  # A hash with connection information for the redis client for cached data
  # The hash can contain any arguments accepted by the constructor for the Redis class in the redis-rb gem
  #
  # *There is ONE notable exception.* Redis sentinel related information must be defined in the redis_sentinels parameter.
  # This is due to a limitation with Logstash configuration directives.
  #
  # Below is a sample data structure making use of redis sentinel and a master named "mymaster".
  # The sentinels will be defined in the example for the redis_sentinels parameter.
  # [source,ruby]
  # ----------------------------------
  # redis_client => {
  #   "url"       => "redis://mymaster"
  #   "password"  => "password"
  # }
  # -----------------------
  #
  # The documentation for the Redis class's constructor can be found at the following URL:
  # https://www.rubydoc.info/gems/redis/Redis#initialize-instance_method
  config :redis_client, :validate => :hash

  # Optional redis sentinels to associate with redis_client.
  # Use host:port syntax
  #
  # Below is an example
  # [source,ruby]
  # ----------------------------------
  # redis_sentinels => [
  #   "sentinel1:26379"
  #   "sentinel2:26379"
  # ]
  # -----------------------
  config :redis_sentinels, :validate => :array

  # @return [Array<String>] The Domo Dataset's columns.
  attr_accessor :dataset_columns

  # @!attribute [r] pipeline_id
  # The Logstash Pipeline ID.
  # @return [String]
  def pipeline_id
    if respond_to?(:execution_context) and execution_context.respond_to?(:pipeline)
      execution_context.pipeline.pipeline_id
    else
      'main'
    end
  end

  # @!attribute [r] lock_key
  # The name of the redis key for the distributed lock.
  # @return [String]
  def lock_key
    "logstash-output-domo:#{@dataset_id}_lock"
  end

  # @!attribute [r] part_num_key
  # The redis key for getting / incrementing the part number.
  # @return [String]
  def part_num_key
    part_num_key = "#{Domo::Queue::REDIS_KEY_PREFIX_FORMAT}" % {:dataset_id => @dataset_id}
    "#{part_num_key}#{Domo::Queue::REDIS_KEY_SUFFIXES[:PART_NUM]}"
  end

  public
  def register
    # @type [Domo::Client] A client connection to Domo's APIs.
    @domo_client = Domo::Client.new(@client_id, @client_secret, @api_host, @api_ssl, Java::ComDomoSdkRequest::Scope::DATA)
    # @type [Java::ComDomoSdkStreamModel::Stream] The Domo Stream associated with our Stream ID and/or Dataset.
    stream = @domo_client.stream(@stream_id, @dataset_id, false)
    # @type [Integer] The Stream ID.
    @stream_id = stream.getId
    # @type [Array<String>] The columns in the Dataset
    @dataset_columns = @domo_client.dataset_schema_columns(stream)
    # Get the dead letter queue writer if it's enabled.
    @dlq_writer = dlq_enabled? ? execution_context.dlq_writer : nil

    # Map of batch jobs (per-thread)
    @thread_batch_map = Concurrent::Hash.new

    # Distributed lock requires a redis queuing mechanism, among other things.
    if @distributed_lock
      if @redis_client.nil?
        raise LogStash::ConfigurationError.new("The redis_client parameter is required when using distributed_lock")
      else
        # Instantiate the redis client for the queue.
        @redis_client = symbolize_redis_client_args(@redis_client)
        unless @redis_sentinels.nil?
          @redis_client[:sentinels] = @redis_sentinels.map do |s|
            host, port = s.split(":")
            {
                :host => host,
                :port => port,
            }
          end
        end
        @redis_client = Redis.new(@redis_client)
      end

      if @lock_hosts.nil? or @lock_hosts.length <= 0
        raise LogStash::ConfigurationError.new("The lock_servers parameter is required when using distributed_lock")
      end

      redlock_options = {
          :retry_count => redlock_retry_count(@lock_retry_delay),
          :retry_delay => (@retry_delay * 1000).to_i,
      }
      @lock_manager = Redlock::Client.new(@lock_hosts, redlock_options)
    else
      @lock_manager = Domo::DummyLockManager.new
    end

    # Simple multi-threaded queue.
    if @redis_client.nil?
      @queue = Concurrent::Hash.new
    # Redis based queue.
    else
      # Attempt to load the queue from redis in case there are still active jobs.
      @queue = Domo::Queue.get_active_queue(@redis_client, @dataset_id, @stream_id, pipeline_id)
    end
  end # def register

  public
  def multi_receive(events)
    # Get or setup a multi-threaded queue if we aren't using redis.
    if @redis_client.nil?
      cur_thread = Thread.current

      if @queue.include? cur_thread
        queue = @queue[cur_thread]
      else
        @queue[cur_thread] = ArrayList.new(events.size)
        queue = @queue[cur_thread]
      end
    # Get or setup a redis queue.
    else
      @queue = Domo::Queue.get_active_queue(@redis_client, @dataset_id, @stream_id, pipeline_id)
      # Create a new Queue (and possibly Stream Execution) if there's no active queue or Stream Execution associated with a queue.
      if @queue.nil? or @queue.execution_id.nil?
        begin
          @queue = @lock_manager.lock!("#{lock_key}", @lock_timeout) do
            # Get or create the Stream Execution.
            stream_execution = @domo_client.stream_client.createExecution(@stream_id)
            # Create the queue.
            Domo::Queue.new(@redis_client, @dataset_id, @stream_id, stream_execution.getId, pipeline_id)
          end
          # If we still don't have a queue at this point, something done messed up real bad.
          if @queue.nil?
            raise LogStash::PluginLoadingError.new("Unable create or locate a queue for DatasetID #{@dataset_id}")
          end
        # Lock acquisition failure is a total showstopper here.
        rescue Redlock::LockError
          # Last chance to get the queue before we crash.
          @queue = Domo::Queue.get_active_queue(@redis_client, @dataset_id, @stream_id, pipeline_id)
          if @queue.nil?
            # Get the list of lock servers for the error message.
            server_list = @lock_manager.instance_variable_get("servers")
            servers = server_list.map do |server|
              server.connection[:host]
            end
            # Stop the show.
            raise LogStash::PluginLoadingError.new("Unable to acquire distributed lock for servers [#{servers.join(', ')}]")
          end
        end
      end

      queue = @queue
    end

    # Get or create a Stream Execution to associate with the queue.
    @lock_manager.lock("#{lock_key}", @lock_timeout) do |locked|
      if locked
        stream_execution = nil
        # Check if the Queue's existing Stream Execution is still valid.
        unless @queue.execution_id.nil?
          stream_execution = @domo_client.stream_client.getExecution(@stream_id, @queue.execution_id)
          if stream_execution.currentState != "ACTIVE"
            stream_execution = nil
          end
        end
        # We didn't find an exectuion so let's make one.
        if stream_execution.nil?
          stream_execution = @domo_client.stream_client.createExecution(@stream_id)
          @queue.execution_id = stream_execution.getId
          _ = @redis_client.getset("#{part_num_key}", "0") unless @redis_client.nil?
        end
      end
    end

    # Initialize part_num as an AtomicInteger if we aren't using redis
    part_num = java.util.concurrent.atomic.AtomicInteger.new(0) if @redis_client.nil?

    events.each do |event|
      break if event == LogStash::SHUTDOWN

      # Encode the Event data and add a job to the queue
      begin
        data = encode_event_data(event)

        # Increment and get the part number from redis or our Atomic Integer.
        if @redis_client.nil?
          part_num.incrementAndGet
        else
          part_num = @redis_client.incr("#{part_num_key}")
        end

        # Create a job for the event + data and add it to the queue.
        job = Domo::Job.new(event, data, part_num, @queue.execution_id)
        queue.add(job)
      # Reject the invalid event and send it to the DLQ if it's enabled.
      rescue ColumnTypeError => e
        unless @dlq_writer.nil?
          @dlq_writer.write(event, e.log_entry)
        end
        @logger.error(e.log_entry,
                      :value       => e.val,
                      :column_name => e.col_name,
                      :data        => e.data,
                      :event       => event,
                      :exception   => e)
      end
    end

    # Process the queue
    batch = queue
    if batch.any?
      send_to_domo(batch)
      # Reset part_num if we aren't using the redis queue.
      if @redis_client.nil?
        batch.clear
        part_num.set(0)
      end
    end
  end
  
  public
  # Send Event data using the DOMO Streams API.
  #
  # @param batch [java.util.ArrayList<Domo::Job>, Domo::Queue] The batch of events to send to DOMO.
  def send_to_domo(batch)
    # Loop through the batch of events until it's cleared out.
    while batch.any?
      # Retriable failures should either be popped back onto the redis queue, or appended to an array if we aren't using redis.
      if batch.is_a? Domo::Queue
        failures = batch
      else
        failures = []
      end

      # Process the jobs.
      batch.each do |job|
        begin
          # Make sure we're still using the correct Stream Execution.
          @lock_manager.lock("#{lock_key}", @lock_timeout) do |locked|
            if locked
              unless @queue.execution_id.nil?
                stream_execution = @domo_client.stream_client.getExecution(@stream_id, @queue.execution_id)
              end

              # We aren't using a valid execution so let's make one.
              if @queue.execution_id.nil? or stream_execution.currentState != "ACTIVE"
                stream_execution = @domo_client.stream_client.createExecution(@stream_id)
              end
              # Update the queue's execution id and reset the part number.
              @queue.execution_id = stream_execution.getId
              _ = @redis_client.getset("#{part_num_key}", "0") unless @redis_client.nil?
            end
          end

          # Upload it
          job.execution_id = @queue.execution_id if job.execution_id.nil?
          @domo_client.stream_client.uploadDataPart(@stream_id, job.execution_id, job.part_num, job.data)
          @logger.debug("Successfully wrote data to DOMO.",
                        :stream_id        => @stream_id,
                        :execution_id     => @queue.execution_id,
                        :queue_pipeline_id => @queue.pipeline_id,
                        :job_id           => job.id,
                        :job_execution_id => job.execution_id,
                        :event            => job.event,
                        :data             => job.data)
        rescue Java::ComDomoSdkRequest::RequestException => e
          if e.getStatusCode < 400 or e.getStatusCode >= 500
            # Queue the job to be retried if we're using the distributed lock or configured to retry.
            if @retry_failures or @distributed_lock
              @logger.info("Got a retriable error from the DOMO Streams API.",
                           :code      => e.getStatusCode,
                           :exception => e,
                           :job_id    => job.id,
                           :event     => job.event,
                           :data      => job.data)
              # If the job's execution id doesn't match our active one, then we need to reset its part number.
              if job.execution_id != @queue.execution_id
                job.execution_id = @queue.execution_id
                job.part_num = @redis_client.incr("#{part_num_key}")
              end
              failures << job
            else
              @logger.error("Encountered a fatal error interacting with the DOMO Streams API.",
                            :code => e.getStatusCode,
                            :exception  => e,
                            :job_id     => job.id,
                            :event      => job.event,
                            :data       => job.data)
            end
          # Something really ain't right so let's give up and optionally write the event to the DLQ.
          else
            log_message = "Encountered a fatal error interacting with the DOMO Streams API."
            @logger.error(log_message,
                          :code       => e.getStatusCode,
                          :exception  => e,
                          :job_id     => job.id,
                          :event      => job.event,
                          :data       => job.data)
            unless @dlq_writer.nil?
              @dlq_writer.write(job.event, "#{log_message} Exception: #{e}")
            end
          end
        end
      end
      # The queue is empty and there are no failures so let's fire off a commit.
      if failures.empty?
        unless @queue.execution_id.nil?
          @lock_manager.lock("#{lock_key}", @lock_timeout) do |locked|
            if locked
              # Validate the active Stream Execution
              stream_execution = @domo_client.stream_client.getExecution(@stream_id, @queue.execution_id)
              if stream_execution.currentState == "ERROR" or stream_execution.currentState == "FAILED"
                @domo_client.stream_client.abortExecution(@stream_id, stream_execution.getId)
              elsif stream_execution.currentState == "ACTIVE"
                @domo_client.stream_client.commitExecution(@stream_id, stream_execution.getId)
                # Clear the queue unless we're using a redis queue.
                if @redis_client.nil?
                  batch.clear
                # Reset the part number if we're getting it from redis.
                else
                  _ = @redis_client.getset("#{part_num_key}", "0")
                end
              end
              # There is no execution to associate with the queue at this point.
              @queue.execution_id = nil
            end
          end

          break
        end
      end
      # Wait and retry failures if we're into that sort of thing.
      if @retry_failures or @distributed_lock
        batch = failures unless batch.is_a? Domo::Queue
        @logger.warn("Retrying DOMO Streams API requests. Will sleep for #{@retry_delay} seconds")
        sleep(@retry_delay)
      end
    end
  end

  public
  def close
    # This can happen somehow.
    return if @queue.nil?
    # Commit or abort the stream execution if that hasn't happened already
    unless @queue.execution_id.nil?
      # We'll hold the lock for a little extra time too just to be safe.
      @lock_manager.lock("#{lock_key}", @lock_timeout*2) do |locked|
        if locked and not @queue.execution_id.nil?
          stream_execution = @domo_client.stream_client.getExecution(@stream_id, @queue.execution_id)

          if stream_execution.currentState == "ERROR" or stream_execution.currentState == "FAILED"
            @domo_client.stream_client.abortExecution(@stream_id, stream_execution.getId)

            @queue.execution_id = nil
            _ = @redis_client.getset("#{part_num_key}", "0") unless @redis_client.nil?
          elsif stream_execution.currentState == "ACTIVE"
            @domo_client.stream_client.commitExecution(@stream_id, stream_execution.getId)

            @queue.execution_id = nil
            _ = @redis_client.getset("#{part_num_key}", "0") unless @redis_client.nil?
          end
          # This looks weird. I know. It's because of our fake news polymorphism on the queue attribute.
          # Clear the queue unless it's got data in it (i.e. another worker is processing events).
          # If we're using a multi-threaded queue, this should always be empty on close, hence the check.
          unless @queue.any?
            @queue.clear
            _ = @redis_client.getset("#{part_num_key}", "0") unless @redis_client.nil?
          end
        end
      end
    end
  end

  public
  # CSV encode event data to pass to DOMO
  #
  # @param event [LogStash::Event] The Event to be sent to DOMO.
  # @return [String] The CSV encoded string.
  def encode_event_data(event)
    column_names = @dataset_columns.map { |col| col[:name] }
    encode_options = {
        :headers => column_names,
        :write_headers => false,
        :return_headers => false,
    }

    csv_data = CSV.generate(String.new, encode_options) do |csv_obj|
      data = event.to_hash.flatten_with_path
      data = data.select { |k, _| column_names.include? k }
      discarded_fields = data.select { |k, _| !column_names.include? k }
      unless discarded_fields.nil? or discarded_fields.length <= 0
        @logger.warn("The event has fields that are not present in the Domo Dataset. They will be discarded.",
                     :fields => discarded_fields,
                     :event  => event)
      end

      @dataset_columns.each do |col|
        # Just extracting this so referencing is as a key in other hashes isn't so damn awkward to read
        col_name = col[:name]
        # Set the column value to null if it's missing from the event data
        unless data.has_key? col_name
          data[col_name] = nil
        end

        # Make sure the type matches what Domo expects.
        if @type_check
          unless data[col_name].nil? or ruby_domo_type_match?(data[col_name], col[:type])
            raise ColumnTypeError.new(col_name, col[:type], data[col_name].class, data[col_name], data)
          end
        end
      end

      data = data.sort_by { |k, _| column_names.index(k) }.to_h
      csv_obj << data.values
    end
    csv_data.strip
  end

  private
  # Calculates an acceptable retry count for trying to acquiring a distributed lock.
  # The default values for the parameters match the defaults in the current version of redlock.
  #
  # @param retry_delay [Integer] The delay between retries (ms).
  # @param minimum_retry_count [Integer] The smallest number of allowable retries.
  # @return [Integer] The number of times redlock can retry acquiring a lock.
  def redlock_retry_count(retry_delay=200, minimum_retry_count=3)
    retry_count = @lock_timeout / retry_delay
    return retry_count unless retry_count < minimum_retry_count
    minimum_retry_count
  end

  private
  # Takes a given value and returns a boolean indicating if its type matches the corresponding Domo column.
  #
  # @param val [Object] The object to inspect.
  # @param domo_column_type [Java::ComDomoSdkDatasetsModel::ColumnType] The Domo column type.
  # @return [Boolean] Whether or not the types match.
  def ruby_domo_type_match?(val, domo_column_type)
    case domo_column_type
    when Java::ComDomoSdkDatasetsModel::ColumnType::DATE
      begin
        _ = Date.parse(val)
        return true
      rescue ArgumentError
        return false
      end
    when Java::ComDomoSdkDatasetsModel::ColumnType::DATETIME
      if val.is_a? LogStash::Timestamp
        return true
      end
      begin
        _ = DateTime.parse(val)
        return true
      rescue ArgumentError
        return false
      end
    when Java::ComDomoSdkDatasetsModel::ColumnType::LONG
      if val.is_a? Integer
        return true
      end
      begin
        _ = Integer(val)
        return true
      rescue ArgumentError
        return false
      end
    when Java::ComDomoSdkDatasetsModel::ColumnType::DOUBLE
      if val.is_a? Float
        return true
      end
      begin
        _ = Float(val)
        return true
      rescue ArgumentError
        return false
      end
    else
      true
    end
  end

  private
  # Builds an array of Redis clients out of the plugin's configuration parameters for distributed locking
  #
  # @return [Array<Redis>]
  def lock_hosts(lock_hosts, lock_ports, lock_passwords)
    lock_servers = Array.new
    lock_hosts.each_with_index do |host, i|
      port = lock_ports.fetch(i, 6379)
      password = lock_passwords.fetch(i, nil)

      lock_servers << Redis.new("host" => host, "port" => port, "password" => password)
    end

    lock_servers
  end

  private
  # Convert all keys in the redis_client hash to symbols because Logstash makes them keys, but redis-rb wants symbols.
  #
  # @param redis_client_args [Hash]
  # @return [Hash]
  def symbolize_redis_client_args(redis_client_args)
    redis_client_args = redis_client_args.inject({}) {|memo, (k, v)| memo[k.to_sym] = v; memo}
    unless redis_client_args.fetch(:sentinels, nil).nil?

      redis_client_args[:sentinels] = redis_client_args[:sentinels].map do |sentinel|
        sentinel.inject({}) {|memo, (k, v)| memo[k.to_sym] = v; memo}
      end
    end

    redis_client_args
  end

  private
  # Checks if the Dead Letter Queue is enabled
  #
  # @return [Boolean]
  def dlq_enabled?
    # Thanks Elasticsearch plugin team!
    # https://github.com/logstash-plugins/logstash-output-elasticsearch/blob/master/lib/logstash/outputs/elasticsearch/common.rb#L349
    # See more in: https://github.com/elastic/logstash/issues/8064
    respond_to?(:execution_context) && execution_context.respond_to?(:dlq_writer) &&
        !execution_context.dlq_writer.inner_writer.is_a?(::LogStash::Util::DummyDeadLetterQueueWriter)
  end

  private
  # Raised when an event field has a type mismatch with a Domo Dataset Column
  class ColumnTypeError < TypeError
    # @return [String]
    attr_reader :col_name
    # @return [Class]
    attr_reader :expected_type
    # @return [Class]
    attr_reader :actual_type
    # @return [Object]
    attr_reader :val
    # @return [Object]
    attr_reader :data

    # A human readable message for error log entries
    # @return [String]
    def log_entry
      "#{@actual_type} is an invalid type for #{@col_name} with the value #{@val}. It should be #{@expected_type}"
    end

    # @param col_name [String]
    # @param expected_type [Class]
    # @param actual_type [Class]
    # @param val [Object]
    # @param data [Hash, nil]
    def initialize(col_name, expected_type, actual_type, val=nil, data=nil)
      @col_name = col_name
      @expected_type = expected_type
      @actual_type = actual_type
      @val = val
      @data = data

      super(log_entry)
    end
  end
end # class LogStash::Outputs::Domo
