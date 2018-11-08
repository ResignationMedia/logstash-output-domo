# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "java"
require "logstash-output-domo_jars.rb"

java_import "com.domo.sdk.streams.model.Stream"
java_import "java.util.concurrent.atomic.AtomicReference"

# An domo output that does nothing.
class LogStash::Outputs::Domo < LogStash::Outputs::Base
  config_name "domo"

  concurrency :single

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
    @domo_stream = @domo_client.get_domo_stream(@stream_id, @dataset_id)

    @pending_queue = Queue.new
    @timer = java.util.Timer.new("DOMO Output #{self.params['id']}", true)

    successes = java.util.concurrent.atomic.AtomicInteger.new(0)
    failures  = java.util.concurrent.atomic.AtomicInteger.new(0)
    retries = java.util.concurrent.atomic.AtomicInteger.new(0)
    
    part_num = java.util.concurrent.atomic.AtomicInteger.new(1)
    @codec.on_event do |event, data|
      @domo_stream_execution = @domo_client.sc.createExecution(@domo_stream.getId())
      @pending_queue << [event, data, part_num, 0]

      send_to_domo(event, data, part_num.get, 0) do |action, event, data, part_num, attempt|
        begin
          action = :failure if action == :retry && !@retry_failures
          case action
            when :success
              successes.incrementAndGet
            when :retry
              retries.incrementAndGet
              next_attempt = attempt + 1
              sleep_for = sleep_for_attempt(next_attempt)
              @logger.info("Retrying DOMO Streams API request. Will sleep for #{sleep_for} seconds")
              timer_task = RetryTimerTask.new(@pending_queue, event, data, part_num, next_attempt)
              @timer.schedule(timer_task, sleep_for * 1000)
            when :failure
              failures.incrementAndGet
            else
              raise "Unknown action #{action}"
          end

          if action == :success || action == :failure
            if successes.get + failures.get == @event_count
              puts "hi"
              @pending_queue << :done
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
        end

        if action == :success
          if @domo_stream_execution.currentState == "ACTIVE"
            @domo_client.sc.commitExecution(@domo_stream.getId(), @domo_stream_execution.getId())
          elsif @domo_stream_execution.currentState == "ERROR" || @domo_stream_execution.currentState == "FAILED"
            @domo_client.sc.abortExecution(@domo_stream.getId(), @domo_stream_execution.getId())
          end
        end
      end

      part_num.incrementAndGet
    end
  end # def register

  #public
  #def process_events
  #  successes = java.util.concurrent.atomic.AtomicInteger.new(0)
  #  failures  = java.util.concurrent.atomic.AtomicInteger.new(0)
  #  retries = java.util.concurrent.atomic.AtomicInteger.new(0)

  #  while popped = @pending_queue.pop
  #    puts popped
  #    break if popped == :done

  #    event, data, part_num, attempt = popped
  #    @domo_stream_execution = @domo_client.sc.createExecution(@domo_stream.getId())

  #    send_to_domo(event, data, part_num, attempt) do |action, event, data, part_num, attempt|
  #      begin
  #        action = :failure if action == :retry && !@retry_failures
  #        case action
  #          when :success
  #            successes.incrementAndGet
  #          when :retry
  #            retries.incrementAndGet
  #            next_attempt = attempt + 1
  #            sleep_for = sleep_for_attempt(next_attempt)
  #            @logger.info("Retrying DOMO Streams API request. Will sleep for #{sleep_for} seconds")
  #            timer_task = RetryTimerTask.new(@pending_queue, event, data, part_num, next_attempt)
  #            @timer.schedule(timer_task, sleep_for * 1000)
  #          when :failure
  #            failures.incrementAndGet
  #          else
  #            raise "Unknown action #{action}"
  #        end

  #        if action == :success || action == :failure
  #          if successes.get + failures.get == @event_count
  #            puts "hi"
  #            @pending_queue << :done
  #          end
  #        end
  #      rescue => e
  #        @logger.error("Error contacting the DOMO API.",
  #          :class => e.class.name,
  #          :message => e.message,
  #          :backtrace => e.backtrace,
  #          :event => event,
  #          :data => data)
  #        failures.incrementAndGet
  #      end
  #    end

  #    if @domo_stream_execution.currentState == "ACTIVE"
  #      @domo_client.sc.commitExecution(@domo_stream.getId(), @domo_stream_execution.getId())
  #    end
  #  end
  #end
  
  public 
  def send_to_domo(event, data, part_num, attempt)
    begin
      @domo_client.sc.uploadDataPart(@domo_stream.getId(), @domo_stream_execution.getId(), part_num, data)
      yield :success, event, data, part_num, attempt
    rescue Java::ComDomoSdkRequest::RequestException => e
      if e.getStatusCode() < 400 && e.getStatusCode() >= 500
        @logger.warn("Got a retriable error from the DOMO Streams API.", 
          :code => e.getStatusCode(), 
          :exception => e)
        yield :retry, event, data, part_num, attempt
      else
        @logger.error("Encountered a fatal error interacting with the DOMO Streams API.", 
          :code => e.getStatusCode(), 
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
    unless @pending_queue.closed?
      @pending_queue.close
    end

    if @domo_stream_execution.currentState == "ACTIVE"
      @domo_client.sc.commitExecution(@domo_stream.getId(), @domo_stream_execution.getId())
    elsif @domo_stream_execution.currentState == "ERROR" || @domo_stream_execution.currentState == "FAILED"
      @domo_client.sc.abortExecution(@domo_stream.getId(), @domo_stream_execution.getId())
    end
  end
  
  public
  def multi_receive(events)
    @event_count = java.util.concurrent.atomic.AtomicInteger.new(events.size)

    events.each do |event|
      @codec.encode(event)
    end
  end

  #public
  #def receive(event)
  #  @codec.encode(event)
  #end # def event

  private
  class LogStashDomo
    attr_reader :c
    attr_reader :sc

    def initialize(client_id, client_secret, api_host, use_https, scopes)
      unless scopes.is_a? Array
        scopes = [scopes]
      end
      client_config = Java::ComDomoSdkRequest::Config.with() 
        .clientId(client_id)
        .clientSecret(client_secret)
        .apiHost(api_host)
        .useHttps(use_https)
        .scope(*scopes)
        .build()
      @c = Java::ComDomoSdk::DomoClient.create(client_config)
      @sc = @c.streamClient()
    end

    def get_domo_stream(stream_id=nil, dataset_id=nil)
      stream_list = @sc.java_method :list, [Java::int, Java::int]
      stream_get = @sc.java_method :get, [Java::long]

      unless stream_id.nil?
        return stream_get.call(stream_id)
      end

      limit = 500
      offset = 0
      streams = stream_list.call(limit, offset)
      until streams.size <= 0
        streams.each do |stream|
          if stream.dataset.getId() == dataset_id
            return stream
          end
        end
        offset += limit
        streams = stream_list.call(limit, offset)
      end
    end
  end

  private
  class RetryTimerTask < java.util.TimerTask
    def initialize(pending_queue, event, data, part_num, attempt)
      @pending = pending
      @event = event
      @data = data
      @part_num = part_num
      @attempt = attempt
      super()
    end

    def run
      @pending << [@event, @data, @part_num, @attempt]
    end
  end

end # class LogStash::Outputs::Domo
