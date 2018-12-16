# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "core_extensions/enumerable/flatten"
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

  # An array of hashes specifying the configuration for the lock servers. Only required if distributed_lock is true.
  #
  # The only required key in the hash is "host", unless the "sentinels" key is provided.
  # If the "sentinels" key is provided, then the "master" key is required.
  # The default redis port (6379) will be used if not provided.
  #
  # An example array with all the accepted keys would be the following:
  # [source,ruby]
  # ----------------------------------
  # lock_servers => [
  #   {
  #     "host"     => "server1"
  #     "port"     => 6379
  #     "password" => "password"
  #   }
  #   {
  #     "host"     => "server2"
  #     "port"     => 6379
  #     "password" => "password2"
  #   }
  #   {
  #     "master"      => "mymaster"
  #     "sentinels" => [
  #       {
  #         "host" => "sentinel1"
  #         "port" => 26379
  #       }
  #     ]
  #   }
  # ]
  # -----------------------
  config :lock_servers, :validate => :array

  # A hash with connection information for the redis client for cached data
  # The hash can contain any arguments accepted by the constructor for the Redis class in the redis-rb gem
  #
  # Below is a sample data structure making use of redis sentinel and a master named "mymaster":
  # [source,ruby]
  # ----------------------------------
  # redis_client => {
  #   "url"       => "redis://mymaster"
  #   "password"  => "password"
  #   "sentinels" => [
  #     {
  #       "host" => "127.0.0.1"
  #       "port" => 26379
  #     }
  #     {
  #       "host" => "sentinel2"
  #       "port" => 26379
  #     }
  #   ]
  # }
  # -----------------------
  #
  # The documentation for the Redis class's constructor can be found at the following URL:
  # https://www.rubydoc.info/gems/redis/Redis#initialize-instance_method
  config :redis_client, :validate => :hash

  attr_accessor :dataset_columns

  public
  def register
    @domo_client = Domo::Client.new(@client_id, @client_secret, @api_host, @api_ssl, Java::ComDomoSdkRequest::Scope::DATA)
    @domo_stream, @domo_stream_execution = @domo_client.stream(@stream_id, @dataset_id, false)
    @dataset_columns = @domo_client.dataset_schema_columns(@domo_stream)

    @dlq_writer = dlq_enabled? ? execution_context.dlq_writer : nil

    # Map of batch jobs (per-thread)
    @thread_batch_map = Concurrent::Hash.new

    # Distributed lock requires a different queuing mechanism, among other things
    if @distributed_lock
      if @redis_client.nil?
        raise LogStash::ConfigurationError.new("The redis_client parameter is required when using distributed_lock")
      else
        @redis_client = symbolize_redis_client_args(@redis_client)
        @redis_client = Redis.new(@redis_client)
      end

      if @lock_servers.nil? or @lock_servers.length <= 0
        raise LogStash::ConfigurationError.new("The lock_servers parameter is required when using distributed_lock")
      else
        redis_clients = @lock_servers.map do |server|
          # Remove the db key from the server because redlock don't care
          server.delete('db')
          redis_client_from_config(server)
        end
      end

      redlock_options = {
          :retry_count => redlock_retry_count(@lock_retry_delay),
          :retry_delay => (@retry_delay * 1000).to_i,
      }
      @lock_manager = Redlock::Client.new(redis_clients, redlock_options)
    end

    # Simple multi-threaded queue
    if @redis_client.nil?
      @queue = Concurrent::Hash.new
    # Redis based queue
    else
      # Attempt to load the queue from redis in case there are still active jobs
      @queue = Domo::Queue.get_active_queue(@redis_client, @dataset_id, @domo_stream.getId)
      unless @queue.nil?
        begin
          # Make sure the Stream Execution is still active in the Domo API.
          stream_execution = @lock_manager.lock!("#{@dataset_id}_lock", @lock_timeout) do
            @domo_client.stream_client.getExecution(@queue.stream_id, @queue.execution_id)
          end

          # Unless it's active, the queue is no longer valid.
          if stream_execution.currentState == "ACTIVE"
            unless @domo_stream_execution.nil?
              # If the Execution returned by the Domo API doesn't match the queue's, then it's invalid.
              if @domo_stream_execution.getId != stream_execution.getId
                @queue = nil
              end
            end
          else
            @queue = nil
          end
        # If we can't get a lock, let's just assume the queue from redis is invalid until it actually matters later.
        rescue Redlock::LockError
          @queue = nil
        end
      end

      # Make a new Queue
      if @queue.nil?
        begin
          @queue = @lock_manager.lock!("#{@dataset_id}_lock", @lock_timeout) do
            @domo_stream, @domo_stream_execution = @domo_client.stream(@stream_id, @dataset_id, false)
            # Get or create the Stream Execution
            if @domo_stream_execution.nil?
              @domo_stream_execution = @domo_client.stream_execution(@domo_stream, @domo_stream_execution)
            end

            Domo::Queue.new(@redis_client, @dataset_id, @domo_stream.getId, @domo_stream_execution.getId)
          end
        # Okay *now* lock acquisition failure is a showstopper
        rescue Redlock::LockError
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
  end # def register
  
  public
  # Send Event data using the DOMO Streams API.
  #
  # @param batch [java.util.ArrayList<Domo::Job>, Domo::Queue] The batch of events to send to DOMO.
  def send_to_domo(batch)
    while batch.any?
      if @queue.is_a? Domo::Queue
        failures = Domo::Queue.new(@redis_client, @dataset_id, @queue.stream_id, @queue.execution_id)
      else
        failures = []
      end

      batch.each do |job|
        begin
          @domo_stream_execution = @domo_client.stream_execution(@domo_stream, @domo_stream_execution)
          @domo_client.stream_client.uploadDataPart(@domo_stream.getId, @domo_stream_execution.getId, job.part_num, job.data)
        rescue Java::ComDomoSdkRequest::RequestException => e
          if e.getStatusCode == -1 || (e.getStatusCode < 400 && e.getStatusCode >= 500)
            unless @domo_stream_execution.nil?
              @domo_stream_execution = @domo_client.stream_execution(@domo_stream, @domo_stream_execution)
            end

            if @retry_failures
              @logger.info("Got a retriable error from the DOMO Streams API.",
                           :code => e.getStatusCode,
                           :exception => e,
                           :event => job.event,
                           :data => job.data)

              failures << job
            else
              @logger.error("Encountered a fatal error interacting with the DOMO Streams API.",
                            :code => e.getStatusCode,
                            :exception => e,
                            :data => data,
                            :event => event)
            end
          else
            log_message = "Encountered a fatal error interacting with the DOMO Streams API."
            @logger.error(log_message,
                          :code => e.getStatusCode,
                          :exception => e,
                          :data => data,
                          :event => event)
            unless @dlq_writer.nil?
              @dlq_writer.write(event, "#{log_message} Exception: #{e}")
            end
          end
        end
      end

      if failures.empty?
        unless @domo_stream_execution.nil?
          # Commit and create a new Stream Execution if that hasn't happened already
          @domo_stream_execution = @domo_client.stream_client.getExecution(@domo_stream.getId, @domo_stream_execution.getId)
          if @domo_stream_execution.currentState == "ACTIVE"
            if @distributed_lock
              @lock_manager.lock("#{@dataset_id}_lock", @lock_timeout) do |locked|
                if locked
                  @domo_client.stream_client.commitExecution(@domo_stream.getId, @domo_stream_execution.getId)
                  @domo_stream_execution = nil
                  _ = @redis_client.getset("#{@dataset_id}_part_num", "0")
                end
              end
            else
              @domo_client.stream_client.commitExecution(@domo_stream.getId, @domo_stream_execution.getId)
              @domo_stream_execution = nil
            end
          end
        end

        break
      end

      if @retry_failures
        batch = failures
        @logger.info("Retrying DOMO Streams API requests. Will sleep for #{@retry_delay} seconds")
        sleep(@retry_delay)
      end
    end
  end

  public
  def close
    # Commit or abort the stream execution if that hasn't happened already
    unless @domo_stream_execution.nil?
      if @distributed_lock
        # We'll hold the lock for a little extra time too just to be safe.
        @lock_manager.lock("#{@dataset_id}_lock", @lock_timeout*2) do |locked|
          if locked
            @domo_stream_execution = @domo_client.stream_client.getExecution(@domo_stream.getId, @domo_stream_execution.getId)

            if @domo_stream_execution.currentState == "ACTIVE"
              @domo_client.stream_client.commitExecution(@domo_stream.getId, @domo_stream_execution.getId)
            elsif @domo_stream_execution.currentState == "ERROR" or @domo_stream_execution.currentState == "FAILED"
              @domo_client.stream_client.abortExecution(@domo_stream.getId, @domo_stream_execution.getId)
            end

            _ = @redis_client.getset("#{@dataset_id}_part_num", "0")
          end
        end
      else
        @domo_stream_execution = @domo_client.stream_client.getExecution(@domo_stream.getId, @domo_stream_execution.getId)

        if @domo_stream_execution.currentState == "ACTIVE"
          @domo_client.stream_client.commitExecution(@domo_stream.getId, @domo_stream_execution.getId)
        elsif @domo_stream_execution.currentState == "ERROR" or @domo_stream_execution.currentState == "FAILED"
          @domo_client.stream_client.abortExecution(@domo_stream.getId, @domo_stream_execution.getId)
        end
      end
    end
  end

  public
  def multi_receive(events)
    # Get or setup a multi-threaded queue if we aren't using redis
    if @redis_client.nil?
      cur_thread = Thread.current

      if @queue.include? cur_thread
        queue = @queue[cur_thread]
      else
        @queue[cur_thread] = ArrayList.new(events.size)
        queue = @queue[cur_thread]
      end
    else
      queue = @queue
    end

    # Initialize part_num as an AtomicInteger if we aren't using redis
    if @redis_client.nil?
      part_num = java.util.concurrent.atomic.AtomicInteger.new(1)
    end
    events.each do |event|
      break if event == LogStash::SHUTDOWN

      # Encode the Event data and add a job to the queue
      begin
        data = encode_event_data(event)

        # Set the Streams API Data Part Number from redis if we're using it
        unless @redis_client.nil?
          part_num = @redis_client.incr("#{@dataset_id}_part_num")
        end

        job = Domo::Job.new(event, data, part_num)
        queue.add(job)

        # Increment the part_num if we aren't using the redis queue.
        if @redis_client.nil?
          part_num.incrementAndGet
        end
      # Reject the event and possibly send it to the DLQ if it's enabled.
      rescue TypeError => e
        unless @dlq_writer.nil?
          @dlq_writer.write(event, e.message)
        end
        @logger.error(e.message,
                      :exception => e,
                      :event     => event)
      end
    end

    # Process the queue
    batch = queue
    if batch.any?
      send_to_domo(batch)
      if @redis_client.nil?
        batch.clear
        part_num.set(1)
      end
    end
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
  # Build a Redis client from hash entries passed by our configuration variables (lock_servers and redis_client)
  #
  # @param server [Hash] An individual server entry in the configuration variable
  # @return [Redis]
  def redis_client_from_config(server)
    password = server.fetch('password', nil)
    sentinels = server.fetch('sentinels', nil)
    db = server.fetch('db', nil)

    if sentinels.nil?
      host = server['host']
      port = server.fetch('port', 6379)

      if db.nil?
        Redis.new(host: host, port: port, password: password)
      else
        Redis.new(host: host, port: port, password: password, db: db)
      end
    else
      url = "redis://#{server['master']}"

      if db.nil?
        Redis.new(url: url, password: password, sentinels: sentinels)
      else
        Redis.new(url: url, password: password, sentinels: sentinels, db: db)
      end
    end
  end

  private
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
            raise TypeError.new("Invalid data type for #{col_name}. It should be #{col[:type]}.")
          end
        end
      end

      data = data.sort_by { |k, _| column_names.index(k) }.to_h
      csv_obj << data.values
    end
    csv_data.strip
  end

  private
  # Takes a given value and returns a boolean indicating if its type matches the corresponding Domo column.
  #
  # @param val [Object] The object to inspect.
  # @param domo_column_type [Java::ComDomoSdkDatasetsModel::ColumnType] The Domo column type.
  # @return [Boolean] The Domo Column Type.
  def ruby_domo_type_match?(val, domo_column_type)
    if domo_column_type == Java::ComDomoSdkDatasetsModel::ColumnType::DATE
      begin
        _ = Date.parse(val)
        return true
      rescue ArgumentError
        return false
      end
    elsif domo_column_type == Java::ComDomoSdkDatasetsModel::ColumnType::DATETIME
      if val.is_a? LogStash::Timestamp
        return true
      end
      begin
        _ = DateTime.parse(val)
        return true
      rescue ArgumentError
        return false
      end
    elsif domo_column_type == Java::ComDomoSdkDatasetsModel::ColumnType::LONG
      if val.is_a? Integer
        return true
      end
      begin
        _ = Integer(val)
        return true
      rescue ArgumentError
        return false
      end
    elsif domo_column_type == Java::ComDomoSdkDatasetsModel::ColumnType::DOUBLE
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
  def lock_servers(lock_hosts, lock_ports, lock_passwords)
    lock_servers = Array.new
    lock_hosts.each_with_index do |host, i|
      port = lock_ports.fetch(i, 6379)
      password = lock_passwords.fetch(i, nil)

      lock_servers << Redis.new("host" => host, "port" => port, "password" => password)
    end

    lock_servers
  end

  private
  # Convert all keys in the redis_client hash to symbols because that's how redis-rb wants them.
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
end # class LogStash::Outputs::Domo
