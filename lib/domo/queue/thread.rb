# encoding: utf-8
require "concurrent/hash"
require "thread"
require "domo/queue"

module Domo
  module Queue
    # PartNumber object to go with the {ThreadedQueue}
    # Uses a java.util.concurrent.atomic.AtomicInteger
    class ThreadPartNumber < PartNumber
      java_import "java.util.concurrent.atomic.AtomicReference"

      # @param initial_value [Integer]
      def initialize(initial_value=0)
        super()
        @part_num = java.util.concurrent.atomic.AtomicInteger.new(initial_value)
      end

      # @return [Integer]
      def incr
        @part_num.incrementAndGet
      end

      # @return [Integer]
      def get
        @part_num.get
      end

      # @param value [Integer]
      def set(value)
        @part_num.set(value)
      end
    end

    # Provides full public API compatibility with Redlock::Client
    # using Mutexes for locking instead of redis instances.
    class ThreadLockManager
      # @return [Concurrent::Hash<Mutex>]
      attr_reader :locks

      # @param locks [Concurrent::Hash, nil]
      def initialize(locks=nil)
        # Hash of our various Mutex locks.
        # The keys correspond to the resource parameter in Redlock::Client locks.
        @locks = locks.nil? ? Concurrent::Hash.new : locks
      end

      # Lock the Mutex named resource and yield or return the status of the lock
      #
      # @param resource [String] The key name in @locks for the Mutex
      # @param extend_life [Boolean] Extend the life of the lock if this thread is holding it.
      # @return [Mutex] The Mutex lock.
      def lock(resource, *args, extend_life: false, **kwargs)
        # Get or create the Mutex
        unless @locks.fetch(resource, nil)&.locked?
          @locks[resource] = Mutex.new
        end
        lock = @locks[resource]
        lock_info = lock_info(lock, resource, extend_life: extend_life)

        if block_given?
          begin
            Proc.new.call(lock_info)
            !!lock
          ensure
            unlock(lock_info)
          end
        else
          lock_info
        end
      end

      # Get a Hash with the same keys as Redlock's lock_info Hash
      #
      # @param lock [Mutex] The associated Mutex lock.
      # @param resource [String] The key in the @locks Hash housing the lock.
      # @param extend_life [Boolean] Don't try to lock the Mutex if the caller is "extending" the lock.
      #   We need this for Redlock compatibility.
      def lock_info(lock, resource, extend_life: false)
        if lock&.locked? and not extend_life
          return false unless lock.try_lock
        end
        unless extend_life
          begin
            lock.lock
          rescue ThreadError => e
            raise e unless e.to_s == 'Mutex relocking by same thread'
          end
        end
        {
            validity: Float::INFINITY,
            resource: resource,
            value: nil
        }
      end

      # Lock the Mutex, run the provided block, and return its results.
      #
      # @return [Object] The results of the block's execution.
      def lock!(*args)
        fail 'No block passed' unless block_given?

        lock(*args) do |lock_info|
          raise ThreadError.new("failed to acquire lock") unless lock_info
          return Proc.new.call(lock_info)
        end
      end

      # Attempt to grab the Mutex lock associated with the provided resource.
      # If the lock doesn't exist, this returns true.
      #   Otherwise the result of the {Mutex#try_lock} method on the associated Mutex is returned.
      #
      # @param resource [String] The lock's name.
      # @return [Boolean]
      def try_lock(resource)
        lock = @locks.fetch(resource, nil)
        return true unless lock
        lock.try_lock
      end

      # Unlock the Mutex associated with the specified lock_info.
      #
      # @param lock_info [Hash] The lock_info Hash associated with the lock. (see {#lock_info})
      def unlock(lock_info, *args)
        return unless lock_info
        lock = @locks.fetch(lock_info[:resource])
        if lock&.locked?
          begin
            lock.unlock
          rescue ThreadError
            # Redlock doesn't freak out if it can't unlock so we shouldn't either
          end
        end
      end
    end

    # A Multi-threaded queue
    class ThreadedQueue
      # @return [String]
      attr_reader :queue_name
      # @return [String]
      attr_reader :pipeline_id
      # @return [String]
      attr_reader :dataset_id
      # @return [Integer]
      attr_reader :stream_id
      # @return [ThreadPartNumber]
      attr_accessor :part_num
      # @return [ThreadLockManager]
      attr_accessor :lock_manager
      # @return [Concurrent::Array]
      attr_accessor :jobs
      # @return [Symbol]
      attr_accessor :commit_status

      # @!attribute [r] last_commit
      # @return [Time]
      def last_commit
        @last_commit
      end

      # @param dataset_id [String]
      # @param stream_id [Integer]
      # @param pipeline_id [String]
      # @param last_commit_time [Time, DateTime, Integer, nil] Either nothing or something parsable into a Time object.
      # @param part_num [ThreadPartNumber, nil]
      def initialize(dataset_id, stream_id=nil, pipeline_id='main', last_commit_time=nil, part_num=nil)
        @dataset_id = dataset_id
        @stream_id = stream_id
        @pipeline_id = pipeline_id

        @lock_manager = ThreadLockManager.new
        @lock_key = "#{@queue_name}_lock"

        # @type [Concurrent::Array]
        @jobs = Concurrent::Array.new

        set_last_commit(last_commit_time)
        if part_num.nil?
          @part_num = ThreadPartNumber.new
        else
          @part_num = part_num
        end
        @commit_status = :open
      end

      # Update the last_commit instance variable as a Time object with variable input data types.
      #
      # @param last_commit [Object] A Time object, nil, or something we can hopefully parse into a Time object.
      def set_last_commit(last_commit=nil)
        if last_commit.nil?
          last_commit = Time.now.utc
        else
          # Do what we can to turn this into a Time
          case last_commit.class
          when DateTime
            last_commit = last_commit.to_time
          when Date
            last_commit = last_commit.to_time
          when String
            last_commit = DateTime.parse(last_commit).to_time
          else
            last_commit = last_commit
          end
        end
        @last_commit = last_commit
      end

      # Clear the queue's execution_id and update the last commit timestamp
      #
      # @param timestamp [Object] Anything that {#set_last_commit} accepts as an input parameter.
      def commit(timestamp=nil)
        @execution_id = nil
        set_last_commit(timestamp)
        clear
      end

      def any?
        !empty?
      end

      def empty?
        length <= 0
      end

      def each
        return enum_for(@jobs.each) unless block_given?
        @jobs.each do |job|
          Proc.new.call(job)
        end
      end

      def each_with_index
        fail 'a block is required' unless block_given?
        @jobs.each_with_index do |job, index|
          Proc.new.call([job, index])
        end
      end

      def clear
        @jobs.clear
      end

      def pop
        @jobs.pop
      end

      def add(job)
        push(job)
      end

      def push(job)
        @jobs << job
      end

      def <<(job)
        push(job)
      end

      def [](index)
        @jobs[index]
      end

      def unshift(job)
        @jobs.unshift(job)
      end

      def length
        @jobs.length
      end

      def size
        length
      end
    end

    module Threaded
      # Threaded JobQueue
      class JobQueue < ThreadedQueue
        # @return [FailureQueue]
        attr_accessor :failures

        # @param dataset_id [String] The Domo Dataset ID.
        # @param stream_id [Integer] The Domo Stream ID.
        # @param execution_id [Integer, nil] The domo Stream Execution ID.
        # @param pipeline_id [String] The Logstash Pipeline ID.
        # @param last_commit_time [Time, DateTime, Integer, nil] Either nothing or something parsable into a Time object.
        def initialize(dataset_id, stream_id=nil, execution_id=nil, pipeline_id='main', last_commit_time=nil)
          @queue_name = "logstash-output-domo:#{@dataset_id}_queue"
          @execution_id = execution_id

          super(dataset_id, stream_id, pipeline_id, last_commit_time)

          @failures = FailureQueue.new(self)
        end

        # @!attribute [r] execution_id
        # @return [Integer]
        def execution_id
          @execution_id.to_i == 0 ? nil : @execution_id.to_i
        end

        # @!attribute [w]
        def execution_id=(execution_id)
          @execution_id = execution_id
        end

        # Clear all jobs in the queue and reset @execution_id
        def clear
          @jobs.clear
          @execution_id = nil
        end
      end

      # A Thread based FailureQueue
      class FailureQueue < ThreadedQueue
        # @return [JobQueue]
        attr_reader :job_queue
        # @return [Symbol]
        attr_accessor :processing_status

        # Redundant method to maintain compatibility with the RedisQueue interface.
        #
        # @param job_queue [JobQueue] The JobQueue associated with the failures.
        # @return [FailureQueue]
        def self.from_job_queue!(job_queue)
          self.new(job_queue)
        end

        # @param job_queue [JobQueue] The JobQueue associated with the failures.
        def initialize(job_queue)
          @queue_name = "logstash-output-domo:#{@dataset_id}_failures"
          @job_queue = job_queue

          super(@job_queue.dataset_id, @job_queue.stream_id, @job_queue.pipeline_id, @job_queue.last_commit, @job_queue.part_num)

          @processing_status = length <= 0 ? :open : :processing
        end

        def <<(job)
          @processing_status = :processing
          push(job)
        end

        def clear
          @processing_status = :open
          @jobs.clear
        end

        # Reprocess failed jobs in this queue and put them back in the main queue.
        #
        # @param stream_execution_id [Integer, nil] The Execution ID the job's should switch to.
        # @return [JobQueue] The new JobQueue.
        def reprocess_jobs!(stream_execution_id=nil)
          @processing_status = :reprocessing
          each do |job|
            if job.execution_id != stream_execution_id
              job.part_num = nil
            end
            job.execution_id = stream_execution_id
            @job_queue << job
          end
          clear
          @job_queue
        end
      end
    end
  end
end