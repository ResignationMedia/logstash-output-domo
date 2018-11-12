# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "java"
require "thread"
require "logstash-output-domo_jars.rb"

java_import "com.domo.sdk.streams.model.Stream"
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

  default :codec, "csv"

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

  public
  def register
    @domo_client = LogStashDomo.new(@client_id, @client_secret, @api_host, @api_ssl, Java::ComDomoSdkRequest::Scope::DATA)
    @domo_stream, @domo_stream_execution = @domo_client.stream(@stream_id, @dataset_id, false )

    # Queue for encoded events needing to be uploaded to DOMO
    @event_queue = Queue.new
    # This Mutex is primarily used for updating objects from the DOMO API
    @event_mutex = Mutex.new
    # Timer for retrying events
    @timer = java.util.Timer.new("DOMO Output #{self.params['id']}", true)
    # The Streams API Data Part Number
    @part_num = java.util.concurrent.atomic.AtomicInteger.new(1)

    @codec.on_event do |event, data|
      job = DomoQueueJob.new(event, data, @part_num, 0)
      @event_queue << job
    end
  end # def register
  
  public
  # Send Event data using the DOMO Streams API.
  # @param event [LogStash::Event] The Event we're processing.
  # @param data [String] The Event's encoded data.
  # @param part_num [java.util.concurrent.atomic.AtomicInteger] The Stream Execution Upload Part Number.
  # @param attempt [Integer] The number of attempts already made to send these data.
  # @yield [Symbol, LogStash::Event, String, java.util.concurrent.atomic.AtomicInteger, Integer] Status of the attempt along with the original provided parameters.
  def send_to_domo(event, data, part_num, attempt)
    begin
      @event_mutex.synchronize do
        # Upload the event data to the Streams API
        @domo_client.stream_client.uploadDataPart(@domo_stream.getId, @domo_stream_execution.getId, part_num, data)
      end
      # Flag the job as a success
      yield :success, event, data, part_num, attempt
    # Handle exceptions from the DOMO SDK.
    rescue Java::ComDomoSdkRequest::RequestException => e
      # Retriable errors
      if e.getStatusCode < 400 && e.getStatusCode >= 500
        @logger.warn("Got a retriable error from the DOMO Streams API.", 
          :code => e.getStatusCode,
          :exception => e)
        yield :retry, event, data, part_num, attempt
      # Fatal errors
      else
        @logger.error("Encountered a fatal error interacting with the DOMO Streams API.", 
          :code => e.getStatusCode,
          :exception => e,
          :data => data,
          :event => event)
        yield :failure, event, data, part_num, attempt
      end
    end
  end

  public
  def close
    @timer.cancel
    unless @event_queue.closed?
      @event_queue.close
    end

    @event_mutex.synchronize do
      # Commit or abort the stream execution if that hasn't happened already
      unless @domo_stream_execution.nil?
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
  # Process the Queue of encoded Events that need to be sent to DOMO.
  def process_queue
    @event_mutex.synchronize do
      # Get or create the Stream Execution
      if @domo_stream_execution.nil?
        @domo_stream_execution = @domo_client.stream_execution(@domo_stream)
      else
        @domo_stream_execution = @domo_client.stream_client.getExecution(@domo_stream.getId, @domo_stream_execution.getId)
        # We need to make a new Stream Execution if the current one has already been aborted or committed
        if @domo_stream_execution == "SUCCESS" or @domo_stream_execution == "FAILURE"
          @domo_stream_execution = @domo_client.stream_execution(@domo_stream)
        end
      end
    end

    successes = java.util.concurrent.atomic.AtomicInteger.new(0)
    failures  = java.util.concurrent.atomic.AtomicInteger.new(0)
    retries = java.util.concurrent.atomic.AtomicInteger.new(0)

    until @event_queue.empty?
      job = @event_queue.pop

      send_to_domo(job.event, job.data, job.part_num.get, job.attempt) do |action, event, data, part_num, attempt|
        begin
          # If we aren't retrying failures, then don't consider retriable errors to be...retriable
          action = :failure if action == :retry && !@retry_failures
          case action
          when :success
            successes.incrementAndGet
          when :retry
            retries.incrementAndGet

            # Calculate how long to sleep before retrying the request
            sleep_for = sleep_for_attempt(next_attempt)
            @logger.info("Retrying DOMO Streams API request. Will sleep for #{sleep_for} seconds")
            # Increment the attempt #
            job.attempt += 1
            # Throw the job onto the timer queue
            timer_task = RetryTimerTask.new(@event_queue, job)
            @timer.schedule(timer_task, sleep_for * 1000)
          when :failure
            failures.incrementAndGet
          else
            raise "Unknown action #{action}"
          end

          # Safeguard in case the queue size gets out of hand with retriable jobs that are destined to fail
          if action == :success || action == :failure
            if successes.get + failures.get == @event_count
              @event_queue.clear
            end
          end
        rescue => e
          @logger.error("Error contacting the DOMO API.",
                        :class => e.class.name,
                        :message => e.message,
                        :backtrace => e.backtrace,
                        :event => event,
                        :data => data)
          failures.incrementAndGet
        # No matter what happens increment the Part Number
        ensure
          @part_num.incrementAndGet
        end
      end
    end

    @event_mutex.synchronize do
      unless @domo_stream_execution.nil?
        # Commit and create a new Stream Execution if that hasn't happened already
        @domo_stream_execution = @domo_client.stream_client.getExecution(@domo_stream.getId, @domo_stream_execution.getId)
        if @domo_stream_execution.currentState == "ACTIVE"
          @domo_client.stream_client.commitExecution(@domo_stream.getId, @domo_stream_execution.getId)
          @domo_stream_execution = nil
        end
      end

      @part_num.set(1)
    end
  end

  public
  def receive(event)
    @event_mutex.synchronize do
      @event_count = java.util.concurrent.atomic.AtomicInteger.new(1 + @event_queue.size)
    end

    @codec.encode(event)
    process_queue
  end

  public
  def multi_receive(events)
    @event_mutex.synchronize do
      @event_count = java.util.concurrent.atomic.AtomicInteger.new(events.size + @event_queue.size)
    end

    events.each do |event|
      @codec.encode(event)
    end
    process_queue
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
  # Calculates how long to randomly sleep when retrying a job.
  # @param attempt [Integer] The number of attempts that have already occurred.
  # @return [Numeric] The number of seconds to wait.
  def sleep_for_attempt(attempt)
    sleep_for = attempt**2
    sleep_for = sleep_for <= 60 ? sleep_for : 60
    (sleep_for/2) + (rand(0..sleep_for)/2)
  end

  private
  # Job that goes into a Queue and handles sending event data using the DOMO Streams API.
  class DomoQueueJob
    # @return [LogStash::Event]
    attr_accessor :event
    # @return [String] A CSV string of the event's data.
    attr_accessor :data
    # @return [Integer] The number of attempts made to send the event to DOMO.
    attr_accessor :attempt

    # @return [Array<Symbol>] Values valid for the action attribute
    def self.valid_actions
      [:new, :retry, :success, :failure]
    end

    # @return [Symbol] The queue action
    def action
      @action
    end

    # @!attribute action [Symbol]
    #   @return [Symbol]
    def action=(action)
      if self.valid_actions.include? action
        @action = action
      else
        raise TypeError("#{action.to_s} is not a valid action.")
      end
    end

    # @return [java.util.concurrent.atomic.AtomicInteger]
    def part_num
      @part_num
    end

    # @!attribute part_num [java.util.concurrent.atomic.AtomicInteger, Integer]
    #   @return [java.util.concurrent.atomic.AtomicInteger]
    def part_num=(part_num)
      if part_num.is_a? java.util.concurrent.atomic.AtomicInteger
        @part_num = part_num
      else
        @part_num = java.util.concurrent.atomic.AtomicInteger.new(part_num)
      end
    end

    # @param event [LogStash::Event]
    # @param data [String]
    # @param part_num [Integer, java.util.concurrent.atomic.AtomicInteger]
    # @param attempt [Integer]
    # @return [DomoQueueJob]
    def initialize(event, data, part_num, attempt)
      @event = event
      @data = data
      @part_num = part_num
      @attempt = attempt

      @action = :new
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

  private
  # Task for retrying a DomoQueueJob
  class RetryTimerTask < java.util.TimerTask
    # @param queue [Queue] The Queue to which the job should be added
    # @param job [DomoQueueJob]
    def initialize(queue, job)
      @queue = queue
      @job = job
      super()
    end

    # Add @job to @queue when the timer goes off.
    # @return [Queue]
    def run
      @queue << @job
    end
  end

end # class LogStash::Outputs::Domo
