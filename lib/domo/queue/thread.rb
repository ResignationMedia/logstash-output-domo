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

    # Emulates the portions of the Redlock::Client API we care about,
    # but using Mutexes for locking.
    # Used with a {ThreadedQueue}.
    class ThreadLockManager
      attr_reader :locks

      # @param locks [Concurrent::Hash, nil]
      def initialize(locks=nil)
        # Hash of our various Mutex locks.
        # The keys correspond to the resource parameter in Redlock::Client locks.
        @locks = locks.nil? ? Concurrent::Hash.new : locks
      end

      # Lock the Mutex named resource and yield or return the status of the lock
      #
      # @param resource [String] The name of the Mutex lock.
      # @return [Mutex] The Mutex lock.
      def lock(resource, *args)
        # Get or create the Mutex
        unless @locks.has_key? resource
          @locks[resource] = Mutex.new
        end
        lock = @locks[resource]

        if block_given?
          begin
            # Acquire the lock and yield the block's result
            Proc.new.call(lock.lock)
            !!lock
          ensure
            lock.unlock if lock&.locked?
          end
        else
          lock
        end
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

      # Returns if the lock doesn't exist.
      # Otherwise the result of the #try_lock method on the Mutex is returned.
      #
      # @param resource [String] The lock's name.
      # @return [Boolean]
      def try_lock(resource)
        lock = @locks.fetch(resource, nil)
        return true unless lock
        lock.try_lock
      end

      # Unlock the Mutex
      #
      # @param resource [String] The lock's name
      def unlock(resource, *args)
        @locks[resource].unlock if @locks[resource]&.locked?
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
      def initialize(dataset_id, stream_id=nil, pipeline_id='main', last_commit_time=nil)
        @dataset_id = dataset_id
        @stream_id = stream_id
        @pipeline_id = pipeline_id

        @lock_manager = ThreadLockManager.new
        @lock_key = "logstash-output-domo:#{@dataset_id}_queue_lock"

        # @type [Concurrent::Array]
        @jobs = Concurrent::Array.new

        set_last_commit(last_commit_time)
        @part_num = ThreadPartNumber.new
        @commit_status = :open
      end

      # Update the last_commit instance variable in a Thread safe manner.
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
        # Update the Queue's last commit in a (mostly) thread-safe manner.
        if @lock_manager
          begin
            @last_commit = @lock_manager.lock!(@lock_key) do |locked|
              last_commit
            end
          rescue ThreadError => e
            if e.message == 'Mutex relocking by same thread'
              @last_commit = last_commit
            else
              raise e
            end
          end
        else
          @last_commit = last_commit
        end
      end

      # Clear the queue's execution_id and update the last commit timestamp
      #
      # @param timestamp [Object] Anything that {set_last_commit} accepts as an input parameter.
      def commit(timestamp=nil)
        set_last_commit(timestamp)

        if @lock_manager
          begin
            @execution_id = @lock_manager.lock!(@lock_key) do |locked|
              if locked
                @execution_id = nil
              end
            end
          rescue ThreadError => e
            if e.message == 'Mutex relocking by same thread'
              @execution_id = nil
            else
              raise e
            end
          end
        else
          @execution_id = nil
        end
      end

      def reprocess_jobs!(jobs, execution_id=nil)
        @jobs = jobs.map do |job|
          if job.execution_id != execution_id
            job.part_num = nil
          end
          job.execution_id = execution_id
          job
        end
      end

      # @!attribute [r] execution_id
      # @return [Integer]
      def execution_id
        @execution_id
      end

      # @!attribute [w]
      def execution_id=(execution_id)
        # Update the Queue's execution_id in a (mostly) thread-safe manner.
        if @lock_manager
          begin
            @execution_id = @lock_manager.lock!(@lock_key) do |locked|
              if locked
                @execution_id = execution_id
              end
            end
          rescue ThreadError => e
            if e.message == 'Mutex relocking by same thread'
              @execution_id = execution_id
            else
              raise e
            end
          end
        else
          @execution_id = execution_id
        end
      end

      def any?
        !empty?
      end

      def empty?
        length <= 0
      end

      def each
        return @jobs.each unless block_given?
        Proc.new.call(@jobs.each)
      end

      def each_with_index
        fail 'a block is required' unless block_given?
        Proc.new.call(@jobs.each_with_index)
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
  end
end