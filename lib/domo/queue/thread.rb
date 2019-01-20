# encoding: utf-8
require "concurrent/hash"

module Domo
  module Queue
    # Emulates the portions of the Redlock::Client API we care about,
    # but using Mutexes for locking.
    # Used with a {ThreadQueue}.
    class ThreadLockManager
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
          return Proc.new.call
        end
      end

      # Unlock the Mutex
      def unlock(resource, *args)
        @locks[resource].unlock if @locks[resource]&.locked?
      end
    end

    # A simple multi-threaded queue that's really just a glorified Concurrent::Hash
    class ThreadQueue < Concurrent::Hash
      java_import "java.util.concurrent.atomic.AtomicReference"

      # @return [String]
      attr_accessor :pipeline_id
      # @return [java.util.concurrent.atomic.AtomicInteger]
      attr_accessor :part_num

      # @!attribute [r] last_commit
      # @return [DateTime]
      def last_commit
        @last_commit
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

      # @param dataset_id [String] The Domo Dataset ID.
      # @param stream_id [Integer] The Domo Stream ID.
      # @param execution_id [Integer, nil] The Domo Stream Execution ID.
      # @param pipeline_id [String] The Logstash Pipeline ID.
      # @param lock_manager [ThreadLockManager, nil] The Thread manager for this queue.
      # @param last_commit [Time, nil] The timestamp of the last commit.
      def initialize(dataset_id, stream_id, execution_id=nil, pipeline_id='main', lock_manager=nil, lock_key=nil, last_commit=nil, *hash_opts)
        super(*hash_opts)

        @dataset_id = dataset_id
        @stream_id = stream_id
        @execution_id = execution_id
        @pipeline_id = pipeline_id
        @lock_manager = lock_manager
        @lock_key = lock_key

        unless last_commit.nil?
          set_last_commit(last_commit)
        end

        @part_num = java.util.concurrent.atomic.AtomicInteger.new(0)
      end
    end
  end
end