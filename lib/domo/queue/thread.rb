# encoding: utf-8
require "concurrent/hash"
require "thread"
require "domo/queue"

module Domo
  module Queue
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
  end
end