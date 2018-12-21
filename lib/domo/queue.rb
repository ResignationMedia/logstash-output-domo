# encoding: utf-8
require "json"
require "redis"

module Domo
  # Manages a redis-based queue for sending Logstash Events to Domo
  # The class is meant to be interchangeable with a "queue" of an ArrayList inside a Concurrent::Hash,
  # which is why it has a ton of redundant methods.
  class Queue
    # Suffixes to add to our redis keys
    REDIS_KEY_SUFFIXES = {
        :ACTIVE_EXECUTION => "_active_execution_id",
        :QUEUE            => "_queue",
    }
    
    # @return [String]
    attr_reader :queue_name
    # @return [Redis]
    attr_reader :client
    # @return [String]
    attr_reader :dataset_id
    # @return [Integer]
    attr_reader :stream_id

    # @!attribute [r] execution_id
    # @return [Integer]
    def execution_id
      execution_id = client.get("#{dataset_id}#{REDIS_KEY_SUFFIXES[:ACTIVE_EXECUTION]}")
      if execution_id.nil? then execution_id else execution_id.to_i end
    end

    # @!attribute [w]
    def execution_id=(execution_id)
      if execution_id.nil?
        @client.del("#{dataset_id}#{REDIS_KEY_SUFFIXES[:ACTIVE_EXECUTION]}")
      else
        @client.set("#{dataset_id}#{REDIS_KEY_SUFFIXES[:ACTIVE_EXECUTION]}", execution_id)
      end
    end

    # Construct a Queue instance related to the active Stream Execution for the provided Dataset.
    #
    # @param redis_client [Redis]
    # @param dataset_id [String]
    # @param stream_id [Integer]
    # @return [Queue]
    def self.get_active_queue(redis_client, dataset_id, stream_id=nil)
      return nil unless redis_client.exists("#{dataset_id}#{REDIS_KEY_SUFFIXES[:QUEUE]}")

      self.new(redis_client, dataset_id, stream_id)
    end

    # Get the active Stream Execution ID for the provided Dataset's Queue.
    #
    # @param redis_client [Redis]
    # @param dataset_id [String]
    def self.get_active_execution_id(redis_client, dataset_id)
      execution_id = redis_client.get("#{dataset_id}#{REDIS_KEY_SUFFIXES[:ACTIVE_EXECUTION]}")
      if execution_id.nil? then execution_id else execution_id.to_i end
    end

    # @param redis_client [Redis]
    # @param dataset_id [String]
    # @param stream_id [Integer, nil]
    # @param execution_id [Integer, nil]
    def initialize(redis_client, dataset_id, stream_id=nil, execution_id=nil)
      # @type [Redis]
      @client = redis_client

      @dataset_id = dataset_id
      @stream_id = stream_id

      if execution_id != self.class.get_active_execution_id(@client, @dataset_id) and not execution_id.nil?
        @client.set("#{dataset_id}#{REDIS_KEY_SUFFIXES[:ACTIVE_EXECUTION]}", execution_id)
      end

      # @type [String]
      @queue_name = "#{dataset_id}#{REDIS_KEY_SUFFIXES[:QUEUE]}"
    end

    # Check if there are any items in the queue.
    #
    # @return [Boolean]
    def any?
      length > 0
    end

    # Check if the queue is empty
    #
    # @return [Boolean]
    def empty?
      !any?
    end

    # Clear the queue. You should probably lock something if you do this.
    #
    # @return [nil]
    def clear
      @client.del(@queue_name)
      @client.del("#{dataset_id}#{REDIS_KEY_SUFFIXES[:ACTIVE_EXECUTION]}")
    end

    # Pop the first job off the queue. Return nil if there are no jobs in the queue.
    #
    # @return [Job, nil]
    def pop
      job = @client.lpop(@queue_name)
      if job.nil?
        return nil
      end
      Job.from_json!(job)
    end

    # Push a DomoQueueJob to the queue
    #
    # @param job [Job] The job to be added.
    def push(job)
      @client.rpush(@queue_name, job.to_json)
    end

    # Alias of #push
    #
    # @param job [Job] The job to be added.
    def <<(job)
      push(job)
    end

    # Alias of #push
    #
    # @param job [Job] The job to be added.
    def add(job)
      push(job)
    end

    # Return the length of the queue.
    #
    # @return [Integer]
    def length
      @client.llen(@queue_name)
    end

    # Alias of #length
    #
    # @return [Integer] The size of the queue.
    def size
      length
    end

    # Iterate over all the jobs in the queue by popping them off the list in redis.
    # *In other words this will call the #pop method!* You've been warned...
    #
    # @param block [Proc]
    # @return [Job]
    def each(&block)
      return enum_for(:each) unless block_given?
      until @client.llen(@queue_name) <= 0
        # yield pop
        job = pop
        yield job unless job.nil?
      end
    end
  end

  # Job that goes into a Queue and handles sending event data using the DOMO Streams API.
  class Job
    # @return [LogStash::Event]
    attr_accessor :event
    # @return [String] A CSV string of the event's data.
    attr_accessor :data
    # @return [Integer]
    attr_accessor :part_num
    # @return [Integer]
    attr_accessor :execution_id

    # @param event [LogStash::Event]
    # @param data [String]
    # @param part_num [Integer, java.util.concurrent.atomic.AtomicInteger]
    # @param execution_id [Integer, nil]
    # @return [Job]
    def initialize(event, data, part_num, execution_id=nil)
      @event = event
      @data = data
      @part_num = part_num
      if @part_num.is_a? java.util.concurrent.atomic.AtomicInteger
        @part_num = @part_num.get
      end
      @execution_id = execution_id
    end

    # Construct the class from a JSON string.
    #
    # @param json_str [String] The JSON string to decode into a constructor.
    # @return [Job]
    def self.from_json!(json_str)
      json_hash = JSON.parse(json_str, {:symbolize_names => true})

      self.new(json_hash[:event], json_hash[:data], json_hash[:part_num], json_hash[:execution_id])
    end

    # Convert the class's (important) attributes to a JSON string.
    # Useful for storing the object in redis (shocker)
    #
    # @return [String] The JSON encoded string.
    def to_json
      json_hash = {
          :event        => @event,
          :data         => @data,
          :part_num     => @part_num,
          :execution_id => @execution_id,
      }

      JSON.generate(json_hash)
    end
  end

  class DummyLockManager
    def initialize(*args)
    end

    def get_lock_info(resource)
      { validity: 0, resource: resource, value: nil }
    end

    def lock(resource, *args, &block)
      lock_info = get_lock_info(resource)
      if block_given?
        yield lock_info
        !!lock_info
      else
        lock_info
      end
    end

    def lock!(*args)
      fail 'No block passed' unless block_given?

      lock(*args) do |lock_info|
        return yield
      end
    end

    def unlock(*args)
    end
  end

  # Raised when there is no associated Stream Execution in redis with the Dataset ID
  class CachedStreamExecutionNotFound < RuntimeError
    # @param msg [String] The error message.
    # @param dataset_id [String] The Domo Dataset ID.
    # @param stream_id [Integer] The Domo Stream ID.
    def initialize(msg, dataset_id, stream_id=nil)
      @dataset_id = dataset_id
      @stream_id = stream_id
      super(msg)
    end
  end
end
