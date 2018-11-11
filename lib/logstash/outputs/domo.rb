# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "java"
require "thread"
require "logstash-output-domo_jars.rb"

java_import "com.domo.sdk.streams.model.Stream"
java_import "java.util.concurrent.atomic.AtomicReference"

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

    @event_queue = Queue.new
    @event_mutex = Mutex.new
    @timer = java.util.Timer.new("DOMO Output #{self.params['id']}", true)

    @part_num = java.util.concurrent.atomic.AtomicInteger.new(1)

    @codec.on_event do |event, data|
      job = DomoQueueJob.new(event, data, @part_num, 0)
      @event_queue << job
    end
  end # def register
  
  public 
  def send_to_domo(event, data, part_num, attempt)
    begin
      @domo_client.stream_client.uploadDataPart(@domo_stream.getId, @domo_stream_execution.getId, part_num, data)
      yield :success, event, data, part_num, attempt
    rescue Java::ComDomoSdkRequest::RequestException => e
      if e.getStatusCode < 400 && e.getStatusCode >= 500
        @logger.warn("Got a retriable error from the DOMO Streams API.", 
          :code => e.getStatusCode,
          :exception => e)
        yield :retry, event, data, part_num, attempt
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
  def process_queue
    @event_mutex.synchronize do
      if @domo_stream_execution.nil?
        @domo_stream_execution = @domo_client.stream_execution(@domo_stream)
      else
        @domo_stream_execution = @domo_client.stream_client.getExecution(@domo_stream.getId, @domo_stream_execution.getId)
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
          action = :failure if action == :retry && !@retry_failures
          case action
          when :success
            successes.incrementAndGet
          when :retry
            retries.incrementAndGet

            sleep_for = sleep_for_attempt(next_attempt)
            @logger.info("Retrying DOMO Streams API request. Will sleep for #{sleep_for} seconds")
            job.attempt += 1
            timer_task = RetryTimerTask.new(@event_queue, job)
            @timer.schedule(timer_task, sleep_for * 1000)
          when :failure
            failures.incrementAndGet
          else
            raise "Unknown action #{action}"
          end

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
        ensure
          @part_num.incrementAndGet
        end
      end
    end

    @event_mutex.synchronize do
      @domo_stream_execution = @domo_client.stream_client.getExecution(@domo_stream.getId, @domo_stream_execution.getId)
      if @domo_stream_execution.currentState == "ACTIVE"
        @domo_client.stream_client.commitExecution(@domo_stream.getId, @domo_stream_execution.getId)
        @domo_stream_execution = @domo_client.stream_client.createExecution(@domo_stream.getId)
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
  class DomoStreamNotFound < RuntimeError
    def initialize(msg, dataset_id=nil, stream_id=nil)
      @dataset_id = dataset_id
      @stream_id = stream_id
      super(msg)
    end
  end

  private
  class DomoQueueJob
    attr_accessor :event
    attr_accessor :data
    attr_accessor :attempt

    def self.valid_actions
      [:new, :retry, :success, :failure]
    end

    def action
      @action
    end

    def action=(action)
      if self.valid_actions.include? action
        @action = action
      else
        raise TypeError("#{action.to_s} is not a valid action.")
      end
    end

    def part_num
      @part_num
    end

    def part_num=(part_num)
      if part_num.is_a? java.util.concurrent.atomic.AtomicInteger
        @part_num = part_num
      else
        @part_num = java.util.concurrent.atomic.AtomicInteger.new(part_num)
      end
    end

    def initialize(event, data, part_num, attempt)
      @event = event
      @data = data
      @part_num = part_num
      @attempt = attempt

      @action = :new
    end
  end

  private
  class LogStashDomo
    attr_reader :client
    attr_reader :stream_client

    def initialize(client_id, client_secret, api_host, use_https, scopes)
      unless scopes.is_a? Array
        scopes = [scopes]
      end
      client_config = Java::ComDomoSdkRequest::Config.with
        .clientId(client_id)
        .clientSecret(client_secret)
        .apiHost(api_host)
        .useHttps(use_https)
        .scope(*scopes)
        .build()
      @client = Java::ComDomoSdk::DomoClient.create(client_config)
      @stream_client = @client.streamClient
    end

    def paginate_list(method, limit, offset, args=Array.new)
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
  class RetryTimerTask < java.util.TimerTask
    def initialize(queue, job)
      @queue = queue
      @job = job
      super()
    end

    def run
      @queue << @job
    end
  end

end # class LogStash::Outputs::Domo
