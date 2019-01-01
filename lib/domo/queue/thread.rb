# encoding: utf-8
require "concurrent/hash"
module Domo
  module Queue
    # Mimics some of the lock methods of Redlock::Client, but using a Mutex.
    # Used with the multi-threaded queue.
    class ThreadLockManager
      # @param semaphore [Mutex, nil]
      def initialize(semaphore=nil)
        # @type [Mutex]
        @semaphore = semaphore.nil? ? Mutex.new : semaphore
      end

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

      def lock!(*args)
        fail 'No block passed' unless block_given?

        lock(*args) do |lock_info|
          return yield
        end
      end

      def unlock(*args)
        @semaphore.unlock if @semaphore.locked?
      end
    end

    class ThreadQueue < Concurrent::Hash
      java_import "java.util.concurrent.atomic.AtomicReference"

      # @return [String]
      attr_accessor :pipeline_id
      # @return [Integer]
      attr_accessor :execution_id
      # @return [java.util.concurrent.atomic.AtomicInteger]
      attr_accessor :part_num

      def initialize(dataset_id, stream_id, execution_id=nil, pipeline_id='main', *hash_opts)
        super(*hash_opts)

        @dataset_id = dataset_id
        @stream_id = stream_id
        @execution_id = execution_id
        @pipeline_id = pipeline_id

        @part_num = java.util.concurrent.atomic.AtomicInteger.new(0)
      end
    end
  end
end