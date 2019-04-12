# encoding: utf-8
require "redis"
require "json"
require "domo/queue"

module Domo
  module Queue
    # Enumerable object that houses {RedisDataPart} objects in redis.
    # The objects are stored in a Sorted Set using the Stream Execution ID as their score.
    class DataPartArray
      include Enumerable

      # @param client [Redis] The Redis client.
      # @param key_name [String] The name of the sorted set's redis key.
      def initialize(client, key_name)
        # @type [Redis]
        @client = client
        # @type [String]
        @key_name = key_name
      end

      # @return [Boolean] Whether or not the set is empty.
      def empty?
        length <= 0
      end

      # Checks if the provided {RedisDataPart} exists in the set
      #
      # @param data_part [RedisDataPart]
      # @return [Boolean]
      def include?(data_part)
        score = @client.zrank(@key_name, data_part.to_json)
        return score.nil? if data_part.execution_id.nil?
        score == data_part.execution_id
      end

      # Add the provided {RedisDataPart} to the set.
      #
      # @param data_part [RedisDataPart]
      def push(data_part)
        return if data_part.nil?
        execution_id = data_part.execution_id
        execution_id = 0 if execution_id.nil?
        _ = @client.zadd(@key_name, execution_id, data_part.to_json)
      end

      alias_method :<<, :push
      alias_method :add, :push

      # Loop through the set and yield its jobs to the provided block.
      #
      # @param execution_id [Integer, nil] If provided, only items in the set whose score matches the execution_id will be yielded.
      def each(execution_id=nil)
        return to_enum(:each, execution_id) unless block_given?

        @client.zscan_each(@key_name) do |d, score|
          next unless execution_id.nil? or execution_id == score
          data_part = RedisDataPart.from_json!(d)
          Proc.new.call(data_part)
        end
      end

      # Loop through the set and yield is jobs and scores (index) to the provided block.
      #
      # @param execution_id [Integer, nil] If provided, only items in the set whose score matches the execution_id will be yielded.
      def each_with_index(execution_id=nil)
        fail 'a block is required' unless block_given?
        index = 0
        @client.zscan_each(@key_name) do |d, score|
          next unless execution_id.nil? or execution_id == score
          data_part = RedisDataPart.from_json!(d)
          Proc.new.call([data_part, index])
          index += 1
        end
      end

      # Implements a basic version of Enumerable#reduce using our redis sorted set.
      #
      # @param accumulator [Object, nil] The starting value. Will be set to the first element in the set if nil.
      # @return [Object] The accumulator as modified by the provided block.
      def reduce(accumulator=nil)
        fail 'a block is required' unless block_given?
        if accumulator.nil?
          skip_first = true
        else
          skip_first = false
        end

        each_with_index do |d, index|
          if index == 0 and skip_first
            accumulator = d
          else
            accumulator = Proc.new.call(accumulator, d)
          end
        end
        accumulator
      end

      # Clear the entire set or clear all items with a score matching the provided execution_id
      #
      # @param execution_id [Integer, nil] If nil, the entire set will be cleared.
      def clear(execution_id=nil)
        unless execution_id.nil?
          return @client.zremrangebyscore(@key_name, execution_id.to_s, execution_id.to_s)
        end
        @client.del(@key_name)
      end

      # The length of the sorted set or a count of the number of items that have a score matching the provided execution_id.
      #
      # @param execution_id [Integer, nil]
      # @return [Integer]
      def length(execution_id=nil)
        return @client.zcard(@key_name) if execution_id.nil?
        @client.zcount(@key_name, execution_id.to_s, execution_id.to_s)
      end

      alias_method :size, :length

      # Get a specific element at the provided index
      #
      # @param index [Integer]
      # @return [RedisDataPart, nil] Returns nil if no matching element was found.
      def [](index)
        return if index + 1 > length
        data_parts = @client.zrange(@key_name, index.to_s, index.to_s)
        return if data_parts.length != 1
        RedisDataPart.from_json!(data_parts[0])
      end
    end

    # An object stored in Redis that allows us to track state on a DataPart associated with a Stream Execution
    class RedisDataPart
      # @return [Symbol] The DataPart's success or failure status.
      attr_accessor :status
      # @return [Integer] The Stream Execution ID associated with this DataPart
      attr_accessor :execution_id
      # @return [Integer] The Part ID.
      attr_reader :part_id

      # Constructor
      #
      # @param part_id [Integer]
      # @param execution_id [Integer, nil]
      # @param status [Symbol]
      def initialize(part_id, execution_id=nil, status=:ready)
        super()
        # @type [Integer]
        @part_id = part_id
        # @type [Integer, nil]
        @execution_id = execution_id
        # @type [Symbol]
        @status = status
      end

      # Convert the object to a JSON string for storage in Redis.
      #
      # @return [String]
      def to_json
        h = {
            :status       => @status,
            :part_id      => @part_id,
            :execution_id => @execution_id,
        }
        JSON.generate(h)
      end

      # Construct the object from a JSON string pulled out of Redis.
      #
      # @param json_str [String] The JSON string.
      # @return [RedisDataPart]
      def self.from_json!(json_str)
        h = JSON.parse(json_str, {:symbolize_names => true})
        self.new(h[:part_id], h[:execution_id], h[:status])
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
          :PENDING          => "_pending",
      }

      # A format string for the redis key prefix
      KEY_PREFIX_FORMAT = "logstash-output-domo:%{dataset_id}:%{stream_id}"

      # @return [String] The name of the queue.
      attr_reader :queue_name
      # @return [String The pipeline id.
      attr_reader :pipeline_id
      # @return [Redis] An initialized redis client
      attr_reader :client
      # @return [String] The redis key for the part number.
      attr_reader :part_num_key
      # @return [DataPartArray] All {RedisDataPart} objects associated with the Queue.
      attr_accessor :data_parts

      # @!attribute [r] execution_id
      # The execution_id associated with the Queue. Should be overridden in the subclass if you actually need it.
      # @return [Integer, nil]
      def execution_id
        nil
      end

      # Update the last_commit timestamp
      #
      # @param timestamp [DateTime, Time, Date, String] Really anything that can be parsed into a Time object.
      def set_last_commit(timestamp=nil)
        if timestamp.nil?
          timestamp = Time.now.utc
        else
          case timestamp
          when DateTime
            timestamp = timestamp.to_time
          when Date
            timestamp = timestamp.to_time
          when String
            timestamp = DateTime.parse(timestamp).to_time
          when Fixnum
            timestamp = Time.at(timestamp.to_i)
          when Integer
            timestamp = Time.at(timestamp)
          when Float
            timestamp = Time.at(timestamp)
          else
            timestamp = timestamp
          end
        end
        # Store the commit time in redis as a UNIX timestamp
        @client.set("#{redis_key_prefix}:last_commit", timestamp.to_i)
      end

      # @!attribute [r] redis_key_prefix
      # The prefix for all redis keys.
      # @return [String]
      def redis_key_prefix
        KEY_PREFIX_FORMAT % {:dataset_id => @dataset_id, :stream_id => @stream_id}
      end

      # @param redis_client [Redis]
      # @param dataset_id [String]
      # @param stream_id [Integer]
      # @param pipeline_id [String, nil]
      def initialize(redis_client, dataset_id, stream_id, pipeline_id='main')
        # @type [Redis]
        @client = redis_client
        # @type [String]
        @dataset_id = dataset_id
        # @type [Integer]
        @stream_id = stream_id
        # @type [String]
        @pipeline_id = pipeline_id

        # @type [String]
        @part_num_key ="#{redis_key_prefix}#{KEY_SUFFIXES[:PART_NUM]}"
        # @type [DataPartArray]
        @data_parts = DataPartArray.new(@client, @part_num_key)
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

      # Clear the queue. Should be implemented in your subclass. You should also probably lock something if you do this.
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

      # Push a Job to the queue
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
        job.data_part = nil unless data_part_valid?(job.data_part)
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

      # Return the Job located at the provided index.
      #
      # @param index [Integer]
      # @return [Job]
      def [](index)
        return if index + 1 > length
        job = @client.lindex(@queue_name, index)
        Job.from_json!(job)
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

      # Iterate over all the jobs in the queue and yield the Job and index to the provided block
      # Unlike #each, the #pop method is *NOT* used so the jobs will not be removed from the queue
      #
      # @return [Array<Job, Integer>]
      def each_with_index
        fail 'a block is required' unless block_given?
        index = 0
        until index + 1 > length
          val = @client.lindex(@queue_name, index)
          job = Job.from_json!(val)
          Proc.new.call([job, index])
          index += 1
        end
      end

      # Specifies whether or not a {RedisDataPart} is valid for the Queue. Needs to be implemented by subclasses.
      def data_part_valid?(*args)
        raise NotImplementedError.new
      end
    end

    module Redis
      # Manages a redis-based queue for sending Logstash Events to Domo
      class JobQueue < RedisQueue
        # @param redis_client [Redis]
        # @param dataset_id [String]
        # @param stream_id [Integer]
        # @param execution_id [Integer, nil]
        # @param pipeline_id [String]
        def initialize(redis_client, dataset_id, stream_id, execution_id=nil, pipeline_id='main')
          super(redis_client, dataset_id, stream_id, pipeline_id)
          # @type [String]
          @queue_name = "#{redis_key_prefix}#{KEY_SUFFIXES[:QUEUE]}"
          set_execution_id(execution_id) unless execution_id.nil?
        end

        # Pop the first job off the queue. Return nil if there are no jobs in the queue.
        #
        # @return [Job, nil]
        def pop
          job = @client.lpop(@queue_name)
          return if job.nil?
          self.processing_status = :processing
          Job.from_json!(job)
        end

        # Prepend a job to the queue. Useful for quick retries
        #
        # @param job [Job] The job to be added.
        def unshift(job)
          @client.lpush(@queue_name, job.to_json)
          self.processing_status = :open
        end

        # Gets the queue associated with an active Stream Execution if it exists. Otherwise a new JobQueue is returned.
        #
        # @param redis_client [Redis]
        # @param dataset_id [String]
        # @param stream_id [Integer]
        # @param pipeline_id [String]
        # @return [JobQueue]
        def self.active_queue(redis_client, dataset_id, stream_id, pipeline_id='main')
          execution_id = self.active_execution(redis_client, dataset_id, stream_id)
          self.new(redis_client, dataset_id, stream_id, execution_id, pipeline_id)
        end

        # Gets the active Stream Execution ID. Returns nil if there isn't one.
        #
        # @param redis_client [Redis]
        # @param dataset_id [String]
        # @param stream_id [Integer]
        # @return [Integer, nil]
        def self.active_execution(redis_client, dataset_id, stream_id)
          key = KEY_PREFIX_FORMAT % {:dataset_id => dataset_id, :stream_id => stream_id}
          key += "#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}"
          id = redis_client.hget(key, "id")
          id.to_i == 0 ? nil : id.to_i
        end

        # @!attribute [r] execution_id
        # The active Stream Execution ID (if available)
        # @return [Integer, nil]
        def execution_id
          id = @client.hget("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "id")
          id.to_i == 0 ? nil : id.to_i
        end

        # @!attribute [w] execution_id
        def execution_id=(execution_id)
          set_execution_id(execution_id)
        end

        # Sets the active Stream Execution ID. The associated redis key will instead be deleted if nil is provided.
        #
        # @param execution_id [Integer, nil]
        def set_execution_id(execution_id)
          old_id = self.execution_id
          if execution_id.nil?
            set_commit_start_time(nil)
            @client.hdel("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "id")
          else
            @client.hset("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "id", execution_id.to_s)
            set_commit_start_time(nil) if old_id != execution_id
          end
          # Blow away all of our (now invalid) DataParts if the Execution ID actually changed.
          unless old_id.nil? or old_id == self.execution_id
            @client.hdel("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "part_id")
            @data_parts.clear
            self.commit_rows = 0
          end
        end

        # Increments the DataPart part_id, creates a new {RedisDataPart} associated with the part_id, adds it to our @data_parts, and returns it.
        #
        # @return [RedisDataPart]
        def incr_data_part
          part_id = @client.hincrby("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "part_id", 1)
          data_part = RedisDataPart.new(part_id, execution_id)
          @data_parts << data_part
          data_part
        end

        # Indicates if the provided {RedisDataPart} is valid for this queue.
        #
        # @param data_part [RedisDataPart]
        # @return [Boolean]
        def data_part_valid?(data_part)
          return false if data_part.nil?
          return false unless @data_parts.include?(data_part)
          return false unless data_part.status.nil? or data_part.status == :ready

          true
        end

        # @!attribute [r] processing_status
        # The status of the queue's job processing
        # It should be :processing whenever we've popped a job from the queue but haven't uploaded it yet.
        # The processing status is reset to :open or nil if a job uploads, fails to upload, or is associated with an invalid Stream Execution.
        #
        # @return [Symbol]
        def processing_status
          status = @client.hget("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "processing_status")
          return if status.nil?
          status.to_sym
        end

        # @!attribute [w] processing_status
        def processing_status=(status)
          if status.nil?
            @client.hdel("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "processing_status")
          else
            @client.hset("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "processing_status", status.to_s)
          end
        end

        # Clear the queue. You should probably lock something if you do this.
        def clear
          @data_parts.clear(execution_id)
          @client.del(@queue_name)
          @client.del("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}")
        end

        # The {FailureQueue} associated with this queue.
        # @return [FailureQueue]
        def failures
          FailureQueue.new(@client, @dataset_id, @stream_id, @pipeline_id)
        end

        # The {PendingJobQueue} associated with this queue.
        # @return [PendingJobQueue]
        def pending_jobs
          PendingJobQueue.new(@client, @dataset_id, @stream_id, @pipeline_id)
        end

        # Checks that all {PendingJobQueue}s and {FailureQueue}s related to this queue are empty.
        #
        # @return [Boolean]
        def all_empty?
          self.empty? and self.pending_jobs.empty? and self.failures.empty? and self.processing_status != :processing
        end

        # Establishes whether or not the queue is empty and that there are no failed jobs and (optionally) no pending jobs.
        #
        # @param include_pending [Boolean] Check that there are no pending jobs as well.
        # @return [Boolean]
        def processed?(include_pending=false)
          return self.all_empty? if include_pending
          self.empty? and self.failures.empty? and self.processing_status != :processing
        end

        # Indicates that the queue is unprocessed (i.e. has jobs). Pending jobs can be taken out of consideration.
        #
        # @param include_pending [Boolean]
        # @return [Boolean]
        def unprocessed?(include_pending=false)
          !self.processed?(include_pending)
        end

        # @!attribute [r] commit_status
        # The status of the commit operation.
        # @return [Symbol]
        def commit_status
          status = @client.hget("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "commit_status")
          status.nil? ? :open : status.to_sym
        end

        # @!attribute [r] commit_rows
        # The number of rows scheduled for this commit
        # @return [Integer]
        def commit_rows
          rows = @client.hget("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "commit_rows")
          rows.to_i
        end

        # Add the provided num of rows to {commit_rows}
        #
        # @param count [Integer]
        def add_commit_rows(count)
          @client.hincrby("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "commit_rows", count)
        end

        # @!attribute [w] commit_rows
        def commit_rows=(count)
          @client.hset("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "commit_rows", count.to_s)
        end

        # Indicates if a commit is complete (i.e. it succeeded or failed)
        #
        # @return [Boolean]
        def commit_complete?
          [:success, :failure].include?(self.commit_status)
        end

        # Indicates if a commit is incomplete (e.g. status is :open)
        #
        # @return [Boolean]
        def commit_incomplete?
          !self.commit_complete?
        end

        # Indicates if a commit has been scheduled.
        #
        # @return [Boolean]
        def commit_scheduled?
          !self.commit_unscheduled?
        end

        # Indicates if a commit has not been scheduled.
        #
        # @return [Boolean]
        def commit_unscheduled?
          self.commit_schedule_time.nil? and self.commit_status != :running
        end

        # Determines if a scheduled commit has stalled out or not.
        #
        # @param commit_delay [Integer] The number of seconds we delay between commits.
        # @return [Boolean]
        def commit_stalled?(commit_delay)
          return self.stuck?(commit_delay) if self.commit_status == :running
          next_commit_time = self.next_commit(commit_delay)

          return false unless self.commit_scheduled?
          schedule_time = self.commit_schedule_time
          return false if next_commit_time.nil?
          return true unless schedule_time.nil? or schedule_time <= next_commit_time + 10
          false
        end

        # @!attribute [r] commit_schedule_time
        # The time at which a commit was scheduled. Returns nil if there isn't a scheduled commit.
        #
        # @return [Time, nil]
        def commit_schedule_time
          scheduled_time = @client.hget("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}",
                                        "commit_schedule_time")
          return if scheduled_time.nil?
          # Convert scheduled_time into a Time object
          begin
            scheduled_time = Integer(scheduled_time)
            scheduled_time = Time.at(scheduled_time).utc
              # Or clear garbage data out of redis and set it to nil
          rescue TypeError => e
            scheduled_time = nil
          end
          scheduled_time
        end

        # @!attribute [w] commit_scheduled_time
        def commit_schedule_time=(scheduled_time)
          if scheduled_time.nil?
            @client.hdel("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "commit_schedule_time")
          else
            @client.hset("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "commit_schedule_time",
                         scheduled_time.to_i)
          end
        end

        # @!attribute [w] commit_status
        def commit_status=(status)
          if [:success, :failure].include?(status)
            set_commit_start_time(nil)
            self.commit_rows = 0
          elsif commit_status != status and status == :running
            set_commit_start_time(Time.now.utc)
          end
          set_execution_id(nil) if status == :failure
          @client.hset("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "commit_status", status.to_s)
        end

        # @!attribute [r] commit_start_time
        # The time at which the current commit operation began
        # @return [Time, nil]
        def commit_start_time
          start_time = @client.hget("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "commit_start_time")
          start_time.to_i == 0 ? nil : Time.at(start_time.to_i).utc
        end

        # Set the commit_start_time
        #
        # @param timestamp [Time, nil]
        def set_commit_start_time(timestamp=nil)
          if timestamp.nil?
            @client.hdel("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "commit_start_time")
          else
            @client.hset("#{redis_key_prefix}#{KEY_SUFFIXES[:ACTIVE_EXECUTION]}", "commit_start_time", timestamp.to_i)
          end
        end

        # Indicates whether or not the queue is likely stuck waiting for a non-existent commit to finish
        #
        # @param commit_timeout [Integer] The number of seconds a commit can run before being considered stuck.
        # @return [Boolean]
        def stuck?(commit_timeout=3600)
          start_time = commit_start_time
          return false unless commit_status == :running
          return false if start_time.nil?
          start_time + commit_timeout <= Time.now.utc
        end

        # Calculate how long to delay a commit
        #
        # @param delay [Integer] The amount of time that must elapse between commits.
        # @return [Integer] The amount of time to wait until the next commit.
        def commit_delay(delay)
          return 0 if last_commit.nil?
          (last_commit + delay) - Time.now.utc
        end

        # The next Time at which a commit is acceptable.
        #
        # @param delay [Integer] The amount of time that must elapse between commits.
        # @return [Time]
        def next_commit(delay)
          return Time.now.utc if last_commit.nil?
          last_commit + commit_delay(delay)
        end

        # @!attribute [r] last_commit
        # The last time a commit was fired. Will be nil if we've never committed before.
        # @return [Time]
        def last_commit
          last_commit = @client.get("#{redis_key_prefix}:last_commit")
          return if last_commit.nil?
          # Convert last_commit into a Time object
          begin
            last_commit = Integer(last_commit)
            last_commit = Time.at(last_commit).utc
              # Or clear garbage data out of redis and set it to nil
          rescue TypeError => e
            last_commit = nil
          end
          last_commit
        end

        # @!attribute [w] last_commit
        def last_commit=(timestamp)
          set_last_commit(timestamp)
        end

        # Clear the queue's execution_id and update the last commit timestamp.
        #
        # @param timestamp [Time, nil]
        def commit(timestamp=nil)
          timestamp = Time.now.utc if timestamp.nil?
          set_execution_id(nil)
          set_last_commit(timestamp)
          self.commit_status = :success
          self.commit_schedule_time = nil
          self.commit_rows = 0
          self.processing_status = :open
          self.pending_jobs.processing_status = :open
        end

        # Notify the queue that a commit has been scheduled.
        #
        # @param timestamp [Time] The time at which the commit was scheduled.
        def schedule(timestamp=nil)
          timestamp ||= Time.now.utc
          self.commit_schedule_time = timestamp
        end

        # Notify the queue that the scheduled commit is in progress.
        #
        # @return [Boolean]
        def run
          self.commit_schedule_time = nil
        end
      end

      # Redis-based Queue for pending jobs.
      # Pending jobs = Jobs whose number of rows is less than their minimum size
      class PendingJobQueue < RedisQueue
        # @param redis_client [Redis]
        # @param dataset_id [String]
        # @param stream_id [Integer]
        # @param pipeline_id [String, nil]
        def initialize(redis_client, dataset_id, stream_id, pipeline_id='main')
          super(redis_client, dataset_id, stream_id, pipeline_id)
          # @type [String]
          @queue_name = "#{redis_key_prefix}#{KEY_SUFFIXES[:PENDING]}"
        end

        def processing_status
          status = @client.get("#{@queue_name}:processing_status")
          return :open if status.nil?
          status.to_sym
        end

        def processing_status=(status)
          if status.nil?
            @client.del("#{@queue_name}:processing_status")
          else
            @client.set("#{@queue_name}:processing_status", status.to_s)
          end
        end

        def ready?
          processing_status == :open
        end

        def push(job)
          @client.rpush(@queue_name, job.to_json)
        end

        # Clear the queue
        def clear
          @client.del(@queue_name)
        end

        # Implements a basic version of Enumerable#reduce except using our Queue and the Jobs in it.
        #
        # @param accumulator [Object, nil] The starting value for the memo. Will be set to the first Job in the queue if nil.
        # @param clear_jobs [Boolean] Whether or not to remove the job from the queue after running our block
        # @return [Object] The accumulator as modified by the provided block.
        def reduce(accumulator=nil, clear_jobs=false)
          fail 'a block is required' unless block_given?
          return nil unless self.processing_status == :merging
          if accumulator.nil?
            skip_first = true
          else
            skip_first = false
          end

          i = 0
          @client.lrange(@queue_name, 0, -1).each do |job|
            job = Job.from_json!(job)
            if i == 0 and skip_first
              accumulator = job.data
              i += 1
              next
            end
            accumulator = Proc.new.call(accumulator, job)
          end
          clear

          accumulator
        end

        # Indicates that the provided {RedisDataPart} is valid
        #
        # @param data_part [RedisDataPart]
        # @return [Boolean]
        def data_part_valid?(data_part)
          # All RedisDataPart objects are valid for this queue.
          true
        end
      end

      # A redis-based queue for failed jobs.
      class FailureQueue < RedisQueue
        # @param redis_client [Redis]
        # @param dataset_id [String]
        # @param stream_id [Integer]
        # @param pipeline_id [String, nil]
        def initialize(redis_client, dataset_id, stream_id, pipeline_id='main')
          super(redis_client, dataset_id, stream_id, pipeline_id)
          # @type [String]
          @queue_name = "#{redis_key_prefix}#{KEY_SUFFIXES[:FAILURE]}"
          if length <= 0
            @client.set("#{@queue_name}_processing_status", :open.to_s)
          end
        end

        def get_job_queue
          JobQueue.active_queue(@client, @dataset_id, @stream_id, @pipeline_id)
        end

        # Add a job to the queue
        #
        # @param job [Job]
        def <<(job)
          job_queue = get_job_queue
          @client.set("#{@queue_name}_processing_status", :processing.to_s)
          push(job)
          job_queue.processing_status = :open
        end

        # Indicates whether or not a {RedisDataPart} is valid for this queue.
        #
        # @param data_part [RedisDataPart]
        # @param job_queue [JobQueue, nil] Validate against an existing {JobQueue}
        # @return [Boolean]
        def data_part_valid?(data_part, job_queue=nil)
          return false if data_part.nil?
          return false if data_part.execution_id.nil?
          return false unless data_part.status == :failed
          unless job_queue.nil?
            return false if job_queue.execution_id != data_part.execution_id
            return job_queue.data_parts.include?(data_part)
          end
          true
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
        # If a Stream Execution ID is provided, then the jobs will have their Execution ID updated and their part numbers reset (if necessary).
        #
        # @return [JobQueue]
        def reprocess_jobs!
          @client.set("#{@queue_name}_processing_status", "reprocessing")
          queue = JobQueue.active_queue(@client, @dataset_id, @stream_id, @pipeline_id)
          each do |job|
            job.data_part = nil unless data_part_valid?(job.data_part, queue)
            job.data_part.status = :ready unless job.data_part.nil?
            queue.add(job)
          end
          queue
        end
      end
    end
  end
end