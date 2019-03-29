# encoding: utf-8
require "redis"
require "domo/queue"

module Domo
  module Queue
    # A redis-based implementation of #{PartNumber}
    class RedisPartNumber < PartNumber
      # Constructor
      #
      # @param client [Redis]
      # @param queue [RedisQueue]
      # @param key_name [String]
      # @param initial_value [Integer, nil]
      def initialize(client, queue, key_name, initial_value=nil)
        super()
        # @type [Redis]
        @client = client
        # @type [String]
        @key_name = key_name
        # @type [RedisQueue]
        @queue = queue
        # @type [Integer]
        @execution_id = queue.execution_id

        if get.nil? or !initial_value.nil?
          initial_value = initial_value.nil? ? 0 : initial_value
          set(initial_value)
        end
      end

      # @return [Integer]
      def incr
        @client.hincrby(@queue.active_execution, @key_name, 1)
      end

      # @return [Integer]
      def get
        @client.hget(@queue.active_execution, @key_name)
      end

      # @param value [Integer, String]
      def set(value)
        @client.hset(@queue.active_execution, @key_name, value.to_s)
      end
    end

    # Base class for any queues that are redis-based
    class RedisQueue
      # Suffixes to add to our redis keys
      KEY_SUFFIXES = {
          :ACTIVE_EXECUTION => "_active_execution",
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
      # @return [Domo::Models::Stream] The Domo Stream
      attr_reader :stream
      # @return [String] The redis key for the part number.
      attr_reader :part_num_key

      attr_accessor :part_num
      attr_accessor :stream_execution

      # @!attribute [r] commit_status
      # The status of the commit operation.
      # @return [Symbol]
      def commit_status
        commit_status = @client.hget("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "commit_status")
        return commit_status.to_sym if commit_status
        :open
      end

      # @!attribute [w] commit_status
      def commit_status=(status)
        @client.hset("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "commit_status", status.to_s)
      end

      # @!attribute [r] redis_key_prefix
      # The prefix for all redis keys.
      # @return [String]
      def redis_key_prefix
        KEY_PREFIX_FORMAT % {:dataset_id => @dataset_id}
      end

      # @!attribute [r] execution_id
      # The active Stream Execution ID (if available)
      # @return [Integer, nil]
      def execution_id
        @stream_execution.execution_id unless @stream_execution.nil?
      end

      # @param redis_client [Redis]
      # @param stream [Domo::Models::Stream]
      # @param pipeline_id [String, nil]
      def initialize(redis_client, stream, pipeline_id='main')
        # @type [Redis]
        @client = redis_client
        # @type [Domo::Models::Stream]
        @stream = stream
        # @type [Domo::Models::StreamExecution]
        @stream_execution = @stream.active_execution
        # @type [String]
        @dataset_id = @stream.dataset_id
        # @type [Integer]
        @stream_id = @stream.stream_id
        # @type [String]
        @pipeline_id = pipeline_id
        # @type [String]
        @part_num_key = "#{redis_key_prefix}#{KEY_SUFFIXES[:PART_NUM]}"

        # part_num = @client.get(@part_num_key)
        # part_num = 0 unless part_num
        @part_num = RedisPartNumber.new(@client, @part_num_key)
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

      # Prepend a job to the queue. Useful for quick retries
      #
      # @param job [Job] The job to be added.
      def unshift(job)
        @client.lpush(@queue_name, job.to_json)
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

      def [](index)
        return if index + 1 > length
        @client.lindex(@queue_name, index)
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
      # @return [Job]
      def each
        fail 'a block is required' unless block_given?
        until @client.llen(@queue_name) <= 0
          job = pop
          break if job.nil?
          Proc.new.call(job)
        end
      end

      def each_with_index
        fail 'a block is required' unless block_given?
        index = 0
        until index + 1 >= length
          val = @client.lindex(@queue_name, index)
          job = Job.from_json!(val)
          Proc.new.call([job, index])
          index += 1
        end
      end

      # Clear the queue's execution_id and update the last commit timestamp.
      #
      # @param timestamp [Time, nil]
      def commit(timestamp=nil)
        @client.hdel("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "id")

        timestamp = Time.now.utc if timestamp.nil?
        set_last_commit(timestamp)
      end
    end

    module Redis
      # Manages a redis-based queue for sending Logstash Events to Domo
      class JobQueue < RedisQueue
        # Find an active JobQueue associated with the provided Stream and Dataset.
        #
        # @param redis_client [Redis] A redis client.
        # @param stream [Domo::Models::Stream]
        # @param pipeline_id [String] The Logstash Pipeline ID.
        # @return [JobQueue]
        def self.active_queue(redis_client, stream, pipeline_id='main')
          redis_key_prefix = KEY_PREFIX_FORMAT % {:dataset_id => stream.dataset_id}

          execution_id = self.active_execution_id(redis_client, stream, pipeline_id)
          self.new(redis_client, stream, execution_id, pipeline_id)
        end

        # Get the active Stream Execution ID for the provided Stream's JobQueue.
        #
        # @param redis_client [Redis] A redis client.
        # @param stream [Domo::Models::Stream]
        # @param pipeline_id [String] The Logstash Pipeline ID.
        # @return [Integer, nil]
        def self.active_execution_id(redis_client, stream, pipeline_id='main')
          redis_key_prefix = KEY_PREFIX_FORMAT % {:dataset_id => stream.dataset_id}
          execution_id = redis_client.hget("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "id")
          execution_id.to_i == 0 ? nil : execution_id.to_i
        end

        # @param redis_client [Redis]
        # @param stream [Domo::Models::Stream]
        # @param pipeline_id [String, nil]
        def initialize(redis_client, stream, pipeline_id='main')
          super(redis_client, stream, pipeline_id)
          # @type [String]
          @queue_name = "#{redis_key_prefix}#{KEY_SUFFIXES[:QUEUE]}"
        end

        # Clear the queue. You should probably lock something if you do this.
        def clear
          @client.del(@queue_name)
          @client.del("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}")
        end

        # The {FailureQueue} associated with this queue.
        def failures
          FailureQueue.new(@client, @stream, @pipeline_id)
        end
      end

      # A redis-based queue for failed jobs.
      class FailureQueue < RedisQueue
        # @param job_queue [JobQueue]
        # @return [FailureQueue]
        def self.from_job_queue!(job_queue)
          self.new(job_queue.client, job_queue.stream, job_queue.pipeline_id)
        end

        # @param redis_client [Redis]
        # @param stream [Domo::Models::Stream]
        # @param pipeline_id [String, nil]
        def initialize(redis_client, stream, pipeline_id='main')
          super(redis_client, stream, pipeline_id)
          # @type [String]
          @queue_name = "#{redis_key_prefix}#{KEY_SUFFIXES[:FAILURE]}"
          if length <= 0
            @client.set("#{@queue_name}_processing_status", :open.to_s)
          end
        end

        def <<(job)
          @client.set("#{@queue_name}_processing_status", :processing.to_s)
          push(job)
        end

        # @!attribute [r] job_queue
        # The Job RedisQueue associated with this RedisQueue.
        # @return [JobQueue]
        def job_queue
          queue = JobQueue.active_queue(@client, @dataset_id, @stream_id, @pipeline_id)
          return queue unless queue.nil?
          JobQueue.new(@client, @stream, nil, @pipeline_id)
        end

        # @!attribute [r] processing_status
        # The status of processing this queue.
        # @return [Symbol]
        def processing_status
          return :open if length <= 0
          processing_status = @client.get("#{@queue_name}_processing_status")
          return processing_status.to_sym if processing_status
          :open
        end

        # @!attribute [w] processing_status
        def processing_status=(status)
          @client.set("#{@queue_name}_processing_status", status.to_s)
        end

        # Clear the queue. You should probably lock something if you do this.
        #
        # @return [nil]
        def clear
          @client.del("#{@queue_name}_processing_status")
          @client.del(@queue_name)
        end

        # Pop all of the jobs out of the queue and add them back to the regular job queue so they can be processed again
        # If a stream_execution_id is provided, then the jobs will have their Execution ID updated and their part numbers reset (if necessary).
        #
        # @param stream_execution_id [Integer, nil] The Stream Execution ID to associated with the new queue.
        # @return [JobQueue]
        def reprocess_jobs!(stream_execution_id=nil)
          @client.set("#{@queue_name}_processing_status", "reprocessing")
          queue = job_queue
          each do |job|
            # if stream_execution_id.nil? or job.execution_id != stream_execution_id
            job.part_num = nil
            # end
            job.execution_id = stream_execution_id
            queue.add(job)
          end
          # Clear this queue out and return the new queue
          clear
          queue
        end
      end
    end
  end
end