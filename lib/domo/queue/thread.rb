# encoding: utf-8
require "concurrent/hash"
module Domo
  module Queue
    # Mimics some of the lock methods of Redlock::Client, but using a Mutex.
    # Used with a {ThreadQueue}.
    class ThreadLockManager
      # @param semaphore [Mutex, nil]
      def initialize(semaphore=nil)
        # @type [Mutex]
        @semaphore = semaphore.nil? ? Mutex.new : semaphore
      end

      # Lock the Mutex and yield or return the status of the lock
      #
      # @return [Boolean] The status of the lock.
      def lock(*args)
        lock = @semaphore.lock
        if block_given?
          begin
            yield lock
            !!lock
          ensure
            @semaphore.unlock if lock
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
          return yield
        end
      end

      # Unlock the Mutex
      def unlock(*args)
        @semaphore.unlock if @semaphore.locked?
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

      def set_last_commit(last_commit=nil)
        if last_commit.nil?
          last_commit = Time.now.utc
        else
          case last_commit.class
          when Time
            last_commit = last_commit.to_datetime
          when Date
            last_commit = last_commit.to_datetime
          when String
            last_commit = DateTime.parse(last_commit)
          else
            last_commit = last_commit
          end
        end
        # Update the Queue's last commit in a (mostly) thread-safe manner.
        if @lock_manager
          begin
            @last_commit = @lock_manager.lock! do |locked|
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

      def commit(timestamp=nil)
        set_last_commit(timestamp)

        if @lock_manager
          begin
            @execution_id = @lock_manager.lock! do |locked|
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
            @execution_id = @lock_manager.lock! do |locked|
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
      # @param execution_id [Integer] The Domo Stream Execution ID.
      # @param pipeline_id [String] The Logstash Pipeline ID.
      def initialize(dataset_id, stream_id, execution_id=nil, pipeline_id='main', lock_manager=nil, last_commit=nil, *hash_opts)
        super(*hash_opts)

        @dataset_id = dataset_id
        @stream_id = stream_id
        @execution_id = execution_id
        @pipeline_id = pipeline_id
        @lock_manager = lock_manager

        unless last_commit.nil?
          set_last_commit(last_commit)
        end

        @part_num = java.util.concurrent.atomic.AtomicInteger.new(0)
      end
    end
  end
end