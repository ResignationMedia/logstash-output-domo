# encoding: utf-8
require "json"
require "redis"

module Domo
  # Manages a redis-based queue for sending Logstash Events to Domo
  # The class is meant to be interchangeable with a "queue" of an ArrayList inside a Concurrent::Hash,
  # which is why it has a ton of redundant methods.
  class Queue
    # @return [String]
    attr_reader :queue_name
    # @return [Redis]
    attr_reader :client
    # @return [String]
    attr_reader :dataset_id
    # @return [Integer]
    attr_reader :stream_id

    # @!attribute [r]
    # @return [Integer]
    def execution_id
      if @execution_id.nil?
        return self.class.get_active_execution_id(@client, @dataset_id)
      end
      @execution_id
    end

    # @!attribute [w]
    # @param execution_id [Integer]
    def execution_id=(execution_id)
      update_redis = false
      if execution_id != @execution_id
        update_redis = true
      end

      @execution_id = execution_id
      if update_redis
        @client.set("#{dataset_id}_active_execution_id", @execution_id)
      end
    end

    # @param redis_client [Redis]
    # @param dataset_id [String]
    # @param stream_id [Integer]
    # @return [Queue]
    def self.active_queue_from_redis(redis_client, dataset_id, stream_id=nil)
      queue = redis_client.get("#{dataset_id}_queue")
      if queue.nil?
        return nil
      end

      execution_id = self.get_active_execution_id(redis_client, dataset_id)
      if execution_id.nil?
        return nil
      end

      self.new(redis_client, dataset_id, stream_id, execution_id)
    end

    # @param redis_client [Redis]
    # @param dataset_id [String]
    def self.get_active_execution_id(redis_client, dataset_id)
      redis_client.get("#{dataset_id}_active_execution_id")
    end

    # @param redis_client [Redis]
    # @param dataset_id [String]
    # @param stream_id [Integer]
    def initialize(redis_client, dataset_id, stream_id=nil, execution_id=nil)
      # @type [Redis]
      @client = redis_client

      @dataset_id = dataset_id
      @stream_id = stream_id

      @execution_id = execution_id
      if @execution_id.nil?
        @execution_id = self.class.get_active_execution_id(@client, @dataset_id)
      elsif @execution_id != self.class.get_active_execution_id(@client, @dataset_id)
        @client.set("#{dataset_id}_active_execution_id", @execution_id)
      end

      if @execution_id.nil?
        raise CachedStreamExecutionNotFound.new("No Stream Execution was found for DatasetID #{@dataset_id} and StreamID #{@stream_id}",
                                                @dataset_id, @stream_id)
      end

      # @type [String]
      @queue_name = "#{dataset_id}_queue_#{@execution_id}"

      # Remove the queue key if it's not a list
      if @client.exists(@queue_name) and @client.type(@queue_name) != 'list'
        @client.del(@queue_name)
      end
    end

    # Check if there are any items in the queue.
    #
    # @return [Boolean]
    def any?
      if @client.llen(@queue_name) > 0
        return true
      end

      false
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
      @client.del("#{dataset_id}_active_execution_id")
    end

    # Pop the first job off the queue. Return nil if there are no jobs in the queue.
    #
    # @return [Job, nil]
    def pop
      _, job = @client.blpop(@queue_name, 5)
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

    # Iterate over all the jobs in the queue by popping them off the list in redis.
    # *In other words this will call the #pop method!* You've been warned...
    #
    # @param block [Proc]
    # @return [Job]
    def each(&block)
      return enum_for(:each) unless block_given?
      while @client.llen(@queue_name) > 0
        yield pop
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

    # @param event [LogStash::Event]
    # @param data [String]
    # @param part_num [Integer, java.util.concurrent.atomic.AtomicInteger]
    # @return [Job]
    def initialize(event, data, part_num)
      @event = event
      @data = data
      @part_num = part_num
      if @part_num.is_a? java.util.concurrent.atomic.AtomicInteger
        @part_num = @part_num.get
      end
    end

    # Construct the class from a JSON string.
    #
    # @param json_str [String] The JSON string to decode into a constructor.
    # @return [Job]
    def self.from_json!(json_str)
      json_hash = JSON.parse(json_str, {:symbolize_names => true})

      self.new(json_hash[:event], json_hash[:data], json_hash[:part_num])
    end

    # Convert the class's (important) attributes to a JSON string.
    # Useful for storing the object in redis (shocker)
    #
    # @return [String] The JSON encoded string.
    def to_json
      json_hash = {
          :event    => @event,
          :data     => @data,
          :part_num => @part_num,
      }

      JSON.generate(json_hash)
    end
  end

  class CachedStreamExecutionNotFound < RuntimeError
    def initialize(msg, dataset_id, stream_id=nil)
      @dataset_id = dataset_id
      @stream_id = stream_id
      super(msg)
    end
  end
end