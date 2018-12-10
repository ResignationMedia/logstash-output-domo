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

      # @param redis_client [Redis]
      # @param queue_name [String]
      def initialize(redis_client, queue_name)
        # @type [Redis]
        @client = redis_client
        # @type [String]
        @queue_name = "#{queue_name}_queue"

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
end