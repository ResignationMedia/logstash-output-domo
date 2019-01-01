# encoding: utf-8
require "redis"

module Domo
  module Queue
    class RedisQueue
      # Suffixes to add to our redis keys
      KEY_SUFFIXES = {
          :ACTIVE_EXECUTION => "_active_execution_id",
          :QUEUE            => "_queue",
          :PART_NUM         => "_part_num",
          :FAILURE          => "_failures",
      }

      # A format string for the redis key prefix
      KEY_PREFIX_FORMAT = "logstash-output-domo:%{dataset_id}"

      # @return [String] The name of the queue.
      attr_reader :queue_name
      # @return [String The pipeline id.
      attr_reader :pipeline_id
      # @return [Redis] An initialized redis client
      attr_reader :client
      # @return [String] The Domo Dataset ID
      attr_reader :dataset_id
      # @return [Integer] The Domo Stream ID
      attr_reader :stream_id
      # @return [String] The redis key for the part number.
      attr_reader :part_num_key

      # @!attribute [r] redis_key_prefix
      # The prefix for all redis keys.
      # @return [String]
      def redis_key_prefix
        KEY_PREFIX_FORMAT % {:dataset_id => @dataset_id}
      end

      # @param redis_client [Redis]
      # @param dataset_id [String]
      # @param stream_id [Integer, nil]
      # @param pipeline_id [String, nil]
      def initialize(redis_client, dataset_id, stream_id=nil, pipeline_id='main')
        # @type [Redis]
        @client = redis_client
        # @type [String]
        @dataset_id = dataset_id
        # @type [Integer]
        @stream_id = stream_id
        # @type [String]
        @pipeline_id = pipeline_id

        # @type [String]
        @part_num_key = "#{redis_key_prefix}#{KEY_SUFFIXES[:PART_NUM]}"
      end

      # Check if there are any items in the queue.
      #
      # @return [Boolean]
      def any?
        length > 0
      end

      # Check if the queue is empty.
      #
      # @return [Boolean]
      def empty?
        !any?
      end

      # Clear the queue. You should probably lock something if you do this.
      #
      # @return [nil]
      def clear
        raise NotImplementedError.new("#clear must be implemented.")
      end

      # Pop the first job off the queue. Return nil if there are no jobs in the queue.
      #
      # @return [Job, nil]
      def pop
        job = @client.lpop(@queue_name)
        return if job.nil?
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
        return to_enum(:each) unless block_given?
        until @client.llen(@queue_name) <= 0
          job = pop
          break if job.nil?
          yield job
        end
      end
    end

    module Redis
      # Manages a redis-based queue for sending Logstash Events to Domo
      # The class is meant to be interchangeable with a "queue" of an ArrayList inside a Concurrent::Hash,
      # which is why it has a ton of redundant methods.
      class JobQueue < RedisQueue
        # @!attribute [r] execution_id
        # The active Stream Execution ID (if available)
        # @return [Integer, nil]
        def execution_id
          execution_id = client.get("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}")
          execution_id.to_i == 0 ? nil : execution_id.to_i
        end

        # @!attribute [w]
        def execution_id=(execution_id)
          if execution_id.nil? or execution_id == 0
            @client.del("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}")
          else
            @client.set("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", execution_id)
          end
        end

        # Find an active JobQueue associated with the provided Stream and Dataset.
        #
        # @param redis_client [Redis] A redis client.
        # @param dataset_id [String] The Domo Dataset ID.
        # @param stream_id [Integer] The Domo Stream ID.
        # @param pipeline_id [String] The Logstash Pipeline ID.
        # @return [JobQueue]
        def self.get_active_queue(redis_client, dataset_id, stream_id=nil, pipeline_id='main')
          redis_key_prefix = KEY_PREFIX_FORMAT % {:dataset_id => dataset_id}
          return nil unless redis_client.exists("#{redis_key_prefix}#{KEY_SUFFIXES[:QUEUE]}")

          execution_id = self.get_active_execution_id(redis_client, dataset_id, pipeline_id)
          self.new(redis_client, dataset_id, stream_id, execution_id, pipeline_id)
        end

        # Get the active Stream Execution ID for the provided Stream's JobQueue.
        #
        # @param redis_client [Redis] A redis client.
        # @param dataset_id [String] The Domo Dataset ID.
        # @param pipeline_id [String] The Logstash Pipeline ID.
        def self.get_active_execution_id(redis_client, dataset_id, pipeline_id='main')
          redis_key_prefix = KEY_PREFIX_FORMAT % {:dataset_id => dataset_id}
          execution_id = redis_client.get("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}")
          execution_id.to_i == 0 ? nil : execution_id.to_i
        end

        # @param redis_client [Redis]
        # @param dataset_id [String]
        # @param stream_id [Integer, nil]
        # @param execution_id [Integer, nil]
        # @param pipeline_id [String, nil]
        def initialize(redis_client, dataset_id, stream_id=nil, execution_id=nil, pipeline_id='main')
          super(redis_client, dataset_id, stream_id, pipeline_id)

          # Set the active Execution ID if it's not nil.
          unless execution_id.nil?
            if execution_id != self.class.get_active_execution_id(@client, @dataset_id)
              @client.set("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", execution_id)
            end
          end

          # @type [String]
          @queue_name = "#{redis_key_prefix}#{KEY_SUFFIXES[:QUEUE]}"
        end

        # Clear the queue. You should probably lock something if you do this.
        #
        # @return [nil]
        def clear
          @client.del(@queue_name)
          @client.del("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}")
        end
      end

      class FailureQueue < RedisQueue
        # @param job_queue [JobQueue]
        # @return [FailureQueue]
        def self.from_job_queue!(job_queue)
          self.new(job_queue.client, job_queue.dataset_id, job_queue.stream_id, job_queue.pipeline_id)
        end

        # @param redis_client [Redis]
        # @param dataset_id [String]
        # @param stream_id [Integer, nil]
        # @param pipeline_id [String, nil]
        def initialize(redis_client, dataset_id, stream_id=nil, pipeline_id='main')
          super(redis_client, dataset_id, stream_id, pipeline_id)

          # @type [String]
          @queue_name = "#{redis_key_prefix}#{KEY_SUFFIXES[:FAILURE]}"
        end

        # @!attribute [r] job_queue
        # The Job RedisQueue associated with this RedisQueue.
        # @return [JobQueue]
        def job_queue
          queue = JobQueue.get_active_queue(@client, @dataset_id, @stream_id, @pipeline_id)
          return queue unless queue.nil?
          JobQueue.new(@client, @dataset_id, @stream_id, nil, @pipeline_id)
        end

        # Clear the queue. You should probably lock something if you do this.
        #
        # @return [nil]
        def clear
          @client.del(@queue_name)
        end

        def reprocess_jobs!(stream_execution_id=nil)
          queue = job_queue
          each do |job|
            if job.execution_id != stream_execution_id
              job.part_num = nil
            end
            job.execution_id = stream_execution_id
            queue.add(job)
          end

          clear
          queue
        end
      end
    end
  end
end