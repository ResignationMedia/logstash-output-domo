# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "core_extensions/enumerable/flatten"
require "concurrent"
require "csv"
require "java"
require "redis"
require "redis/distributed"
require "redlock"
require "thread"
require "logstash-output-domo_jars.rb"

# Add a method to Enumerable to flatten complex data structures
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

  # Use a distributed lock. Necessary when running the plugin against the same Dataset on multiple Logstash servers.
  config :distributed_lock, :validate => :boolean, :default => false

  # The timeout on holding locks in milliseconds.
  # It is not recommended to hold the lock for less than 3,000ms due to lag on the DOMO API.
  config :lock_timeout, :validate => :number, :default => 3000

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

  # An ID for this instance. Will be autogenerated if not specified.
  config :id, :validate => :string

  attr_accessor :dataset_columns

  public
  def register
    @domo_client = LogStashDomo.new(@client_id, @client_secret, @api_host, @api_ssl, Java::ComDomoSdkRequest::Scope::DATA)
    @domo_stream, @domo_stream_execution = @domo_client.stream(@stream_id, @dataset_id, false)
    @dataset_columns = @domo_client.dataset_schema_columns(@domo_stream)

    # Map of batch jobs (per-thread)
    @thread_batch_map = Concurrent::Hash.new

    if @distributed_lock
      if @redis_client.nil?
        raise LogStash::ConfigurationError("The redis_client parameter is required when using distributed_lock")
      else
        @redis_client = symbolize_redis_client_args(@redis_client)
        @redis_client = Redis.new(@redis_client)
      end

      if @lock_servers.nil? or @lock_servers.length <= 0
        raise LogStash::ConfigurationError("The lock_servers parameter is required when using distributed_lock")
      else
        redis_clients = @lock_servers.map do |server|
          # Remove the db key from the server because redlock don't care
          server.delete('db')
          redis_client_from_config(server)
        end
      end

      @lock_manager = Redlock::Client.new(redis_clients)
    end
  end # def register
  
  public
  # Send Event data using the DOMO Streams API.
  #
  # @param batch [java.util.ArrayList<DomoQueueJob>] The batch of events to send to DOMO.
  def send_to_domo(batch)
    while batch.any?
      failures = []
      batch.each do |job|
        begin
          @domo_stream_execution = @domo_client.stream_execution(@domo_stream, @domo_stream_execution)
          @domo_client.stream_client.uploadDataPart(@domo_stream.getId, @domo_stream_execution.getId, job.part_num, job.data)
          puts job.part_num
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
          # TODO: Implement DLQ?
          else
            @logger.error("Encountered a fatal error interacting with the DOMO Streams API.",
                          :code => e.getStatusCode,
                          :exception => e,
                          :data => data,
                          :event => event)
          end
        end
      end
      
      if failures.empty?
        unless @domo_stream_execution.nil?
          # Commit and create a new Stream Execution if that hasn't happened already
          @domo_stream_execution = @domo_client.stream_client.getExecution(@domo_stream.getId, @domo_stream_execution.getId)
          if @domo_stream_execution.currentState == "ACTIVE"
            @domo_client.stream_client.commitExecution(@domo_stream.getId, @domo_stream_execution.getId)
            @domo_stream_execution = nil
          end
        end

        _ = @redis_client.getset("#{@dataset_id}_part_num", "0")
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
      @domo_stream_execution = @domo_client.stream_client.getExecution(@domo_stream.getId, @domo_stream_execution.getId)

      if @domo_stream_execution.currentState == "ACTIVE"
        @domo_client.stream_client.commitExecution(@domo_stream.getId, @domo_stream_execution.getId)
      elsif @domo_stream_execution.currentState == "ERROR" or @domo_stream_execution.currentState == "FAILED"
        @domo_client.stream_client.abortExecution(@domo_stream.getId, @domo_stream_execution.getId)
      end

      _ = @redis_client.getset("#{@dataset_id}_part_num", "0")
    end
  end

  public
  def multi_receive(events)
    cur_thread = Thread.current
    unless @thread_batch_map.include? cur_thread
      @thread_batch_map[cur_thread] = ArrayList.new(events.size)
    end

    events.each do |event|
      break if event == LogStash::SHUTDOWN
      # The Streams API Data Part Number
      part_num = @redis_client.incr("#{@dataset_id}_part_num")
      # Encode the Event data and add a job to the queue
      data = encode_event_data(event)
      job = DomoQueueJob.new(event, data, part_num)
      @thread_batch_map[Thread.current].add(job)
    end

    batch = @thread_batch_map[cur_thread]
    if batch.any?
      send_to_domo(batch)
      batch.clear
    end
  end

  public
  # Raised if a DOMO Stream cannot be found in the API.
  class DomoStreamNotFound < RuntimeError
    # @param msg [String] The error message.
    # @param dataset_id [String, nil] The DOMO DatasetID.
    # @param stream_id [Integer, nil] The DOMO StreamID.
    # @return [DomoStreamNotFound]
    def initialize(msg, dataset_id=nil, stream_id=nil)
      @dataset_id = dataset_id
      @stream_id = stream_id
      super(msg)
    end
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
    encode_options = {
        :headers => @dataset_columns,
        :write_headers => false,
        :return_headers => false,
    }

    csv_data = CSV.generate(String.new, encode_options) do |csv_obj|
      data = event.to_hash.flatten_with_path
      data = data.select { |k, _| @dataset_columns.include? k }
      @dataset_columns.each do |col|
        unless data.has_key? col
          data[col] = nil
        end
      end

      data = data.sort_by { |k, _| @dataset_columns.index(k) }.to_h
      csv_obj << data.values
    end
    csv_data.strip
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
  # Job that goes into a Queue and handles sending event data using the DOMO Streams API.
  class DomoQueueJob
    # @return [LogStash::Event]
    attr_accessor :event
    # @return [String] A CSV string of the event's data.
    attr_accessor :data
    # @return [Integer]
    attr_accessor :part_num

    # @param event [LogStash::Event]
    # @param data [String]
    # @param part_num [Integer]
    # @return [DomoQueueJob]
    def initialize(event, data, part_num)
      @event = event
      @data = data
      @part_num = part_num
    end
  end

  private
  # Interacts with the DOMO APIs.
  class LogStashDomo
    # @return [Java::ComDomoSdk::DomoClient]
    attr_reader :client
    # @return [Java::ComDomoSdkStreams::StreamClient]
    attr_reader :stream_client

    # @param client_id [String] The OAuth ClientID.
    # @param client_secret [String] The OAuth Client Secret.
    # @param api_host [String] The host for API connections.
    # @param use_https [Boolean] Use HTTPS for API requests.
    # @param scopes [Array<Java::ComDomoSdkRequest::Scope>, Java::ComDomoSdkRequest::Scope] The OAuth permission scopes.
    # @return [LogStashDomo]
    def initialize(client_id, client_secret, api_host, use_https, scopes)
      unless scopes.is_a? Array
        scopes = [scopes]
      end
      # Build the API client configuration
      client_config = Java::ComDomoSdkRequest::Config.with
        .clientId(client_id)
        .clientSecret(client_secret)
        .apiHost(api_host)
        .useHttps(use_https)
        .scope(*scopes)
        .build()
      # Instantiate our clients
      @client = Java::ComDomoSdk::DomoClient.create(client_config)
      @stream_client = @client.streamClient
    end

    # Enumerator that lazily paginates through various DOMO SDK methods that make use of it.
    #
    # @param method [Method] The DOMO SDK method to be called
    # @param limit [Integer] The limit of results per page
    # @param offset [Integer] The page offset
    # @param args [Array] An array of arguments to be passed to `method`
    # @return [Object] A result from the API call
    def paginate_list(method, limit, offset=0, args=Array.new)
      args << limit
      Enumerator.new do |y|
        results = method.call(*args, offset)
        until results.size <= 0
          results.each do |result|
            y.yield result
          end
          offset += limit
          results = method.call(*args, offset)
        end
      end
    end

    # Get column names from the provided Stream's Dataset
    #
    # @param stream [Java::ComDomoSdkStreamModel::Stream] A DOMO SDK Stream object
    # @return [Array<String>] The Dataset's column names
    def dataset_schema_columns(stream)
      dataset = @client.dataSetClient.get(stream.getDataset.getId)
      schema = dataset.getSchema

      schema.getColumns.map(&:getName)
    end

    # Get the provided Stream's ACTIVE Stream Execution or create a new one
    #
    # @param stream [Java::ComDomoSdkStreamModel::Stream] A DOMO SDK Stream object
    # @param stream_execution [Java::ComDomoSdkStreamModel::Execution, nil] If provided, check for the latest state on the Stream Execution before creating a new one.
    # @return [Java::ComDomoSdkStreamModel::Execution]
    def stream_execution(stream, stream_execution=nil)
      create_execution = @stream_client.java_method :createExecution, [Java::long]
      if stream_execution.nil?
        limit = 50
        offset = 0
        list_executions = @stream_client.java_method :listExecutions, [Java::long, Java::long, Java::long]

        paginate_list(list_executions, limit, offset, [stream.getId]).each do |execution|
          if execution.currentState == "ACTIVE"
            return execution
          end
        end
      else
        stream_execution = @stream_client.getExecution(stream.getId, stream_execution.getId)
        if stream_execution.currentState == "ACTIVE"
          return stream_execution
        end
      end

      create_execution.call(stream.getId)
    end

    # Get a Stream
    #
    # @param stream_id [Integer, nil] The optional ID of the Stream
    # @param dataset_id [String, nil] The ID of the associated Dataset
    # @param include_execution [Boolean] Returns a new or active Stream Execution along with the Stream
    # @return [Array<Java::ComDomoSdkStreamModel::Stream, (Java::ComDomoSdkStreamModel::Execution, nil)>]
    def stream(stream_id=nil, dataset_id=nil, include_execution=true)
      stream_list = @stream_client.java_method :list, [Java::int, Java::int]
      stream_get = @stream_client.java_method :get, [Java::long]

      unless stream_id.nil?
        return stream_get.call(stream_id)
      end

      limit = 500
      offset = 0
      stream = nil
      paginate_list(stream_list, limit, offset).each do |s|
        if s.dataset.getId == dataset_id
          stream = s
          break
        end
      end

      if stream.nil?
        raise DomoStreamNotFound("No Stream found for Dataset #{dataset_id}", dataset_id, stream_id)
      end

      if include_execution
        stream_execution = stream_execution(stream)
      else
        stream_execution = nil
      end
      return stream, stream_execution
    end
  end
end # class LogStash::Outputs::Domo
