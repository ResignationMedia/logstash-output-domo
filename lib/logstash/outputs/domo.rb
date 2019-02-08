# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "core_extensions/flatten"
require "concurrent"
require "csv"
require "java"
require "redis"
require "redlock"
require "thread"
require "domo/client"
require "domo/queue/redis"
require "domo/queue/thread"
require "logstash-output-domo_jars.rb"

# Add a method to Enumerable to flatten complex data structures into CSV columns
Hash.include CoreExtensions::Flatten

java_import "com.domo.sdk.streams.model.Stream"
java_import "java.util.ArrayList"
java_import "java.util.concurrent.atomic.AtomicReference"

# Write events to a DOMO Streams Dataset.
#
# Requires DOMO OAuth credentials.
# https://developer.domo.com/docs/authentication/overview-4
#
# Additionally, the DOMO Streams API requires that the Dataset and Stream be made via the API.
# Streams cannot be added to existing Datasets.
# https://developer.domo.com/docs/stream/overview
class LogStash::Outputs::Domo < LogStash::Outputs::Base
  config_name "domo"

  concurrency :shared

  # OAuth ClientID
  config :client_id, :validate => :string, :required => :true

  # OAuth Client Secret
  config :client_secret, :validate => :string, :required => :true

  # The DOMO StreamID. If unknown, use the dataset_id parameter instead.
  # This parameter is preferred for performance reasons.
  config :stream_id, :validate => :number

  # The DOMO DataSetID. 
  # Will be used to lookup or create a Stream if the stream_id parameter is not specified.
  config :dataset_id, :validate => :string, :required => :true

  # The hostname for API requests
  config :api_host, :validate => :string, :default => "api.domo.com"

  # Use TLS for API requests
  config :api_ssl, :validate => :boolean, :default => true

  # The amount of time (seconds) to wait between Stream commits.
  # Data will continue to be uploaded until the delay has passed and the queue has empty.
  # Domo Support recommends setting this to at least 15 minutes.
  # Set to 0 (default) to disable.
  config :commit_delay, :validate => :number, :default => 0

  # An optional field for adding an upload timestamp to the data.
  # This field must be defined as a DATETIME in the Dataset's Schema.
  # The field is disabled by default.
  config :upload_timestamp_field, :validate => :string

  # An optional field for adding a partition column to the data.
  # This allows the Stream Dataset to be used with Domo's Data Assembler
  # Currently, it just sets the column to the current *date*
  config :partition_field, :validate => :string

  # Retry failed requests. Enabled by default. It would be a pretty terrible idea to disable this. 
  config :retry_failures, :validate => :boolean, :default => true

  # The delay (seconds) on retrying failed requests
  config :retry_delay, :validate => :number, :default => 2

  # Ensure that Event fields have the same data type as their corresponding columns in Domo.
  config :type_check, :validate => :boolean, :default => true

  # Use a distributed lock. Necessary when running the plugin against the same Dataset on multiple Logstash servers.
  config :distributed_lock, :validate => :boolean, :default => false

  # The timeout on holding locks in milliseconds.
  # It is not recommended to hold the lock for less than 3,000ms due to lag on the DOMO API.
  config :lock_timeout, :validate => :number, :default => 3000

  # The delay (ms) on retrying acquiring the lock.
  # The default matches the default for the distributed locking library (redlock)
  config :lock_retry_delay, :validate => :number, :default => 200

  # An array of redis hosts (using the redis:// URI syntax) to connect to for the distributed lock
  #
  # *REDIS SENTINEL IS NOT SUPPORTED*
  # Don't like it? Ask Elastic to make Logstash support arrays of hashes in config files.
  #
  # An example array showcasing the various redis URI options can be found below:
  # [source,ruby]
  # ----------------------------------
  # lock_hosts => [
  #   "redis://127.0.0.1:6379"
  #   "redis://:password@host2:6379"
  #   "redis://host3:6379/0"
  #   "unix+redis://some/socket/path?db=0&password=password"
  # ]
  # -----------------------
  config :lock_hosts, :validate => :array

  # A hash with connection information for the redis client for cached data
  # The hash can contain any arguments accepted by the constructor for the Redis class in the redis-rb gem
  #
  # *There is ONE notable exception.* Redis sentinel related information must be defined in the redis_sentinels parameter.
  # This is due to a limitation with Logstash configuration directives.
  #
  # Below is a sample data structure making use of redis sentinel and a master named "mymaster".
  # The sentinels will be defined in the example for the redis_sentinels parameter.
  # [source,ruby]
  # ----------------------------------
  # redis_client => {
  #   "url"       => "redis://mymaster"
  #   "password"  => "password"
  # }
  # -----------------------
  #
  # The documentation for the Redis class's constructor can be found at the following URL:
  # https://www.rubydoc.info/gems/redis/Redis#initialize-instance_method
  config :redis_client, :validate => :hash

  # Optional redis sentinels to associate with redis_client.
  # Use host:port syntax
  #
  # Below is an example
  # [source,ruby]
  # ----------------------------------
  # redis_sentinels => [
  #   "sentinel1:26379"
  #   "sentinel2:26379"
  # ]
  # -----------------------
  config :redis_sentinels, :validate => :array

  # @return [Array<String>] The Domo Dataset's columns.
  attr_accessor :dataset_columns

  # @!attribute [r] pipeline_id
  # The Logstash Pipeline ID.
  # @return [String]
  def pipeline_id
    if respond_to?(:execution_context) and execution_context.respond_to?(:pipeline)
      execution_context.pipeline.pipeline_id
    else
      'main'
    end
  end

  # @!attribute [r] lock_key
  # The name of the redis key for the distributed lock.
  # @return [String]
  def lock_key
    "logstash-output-domo:#{@dataset_id}_lock"
  end

  # @!attribute [r] commit_lock_key
  # The name of the redis key for locking the current commit.
  # @return [String]
  def commit_lock_key
    "logstash-output-domo:#{@dataset_id}_commit_lock"
  end

  # @!attribute [r] part_num_key
  # The redis key for getting / incrementing the part number.
  # @return [String]
  def part_num_key
    if @queue.is_a? Domo::Queue::RedisQueue
      return @queue.part_num_key
    end
    part_num_key = "#{Domo::Queue::RedisQueue::KEY_PREFIX_FORMAT}" % {:dataset_id => @dataset_id}
    "#{part_num_key}#{Domo::Queue::RedisQueue::KEY_SUFFIXES[:PART_NUM]}"
  end

  # Establishes whether or not the queue is empty.
  # @return [Boolean]
  def queue_processed?
    @queue.length <= 0
  end

  public
  def register
    # @type [Domo::Client] A client connection to Domo's APIs.
    @domo_client = Domo::Client.new(@client_id, @client_secret, @api_host, @api_ssl, Java::ComDomoSdkRequest::Scope::DATA)
    # @type [Java::ComDomoSdkStreamModel::Stream] The Domo Stream associated with our Stream ID and/or Dataset.
    stream = @domo_client.stream(@stream_id, @dataset_id, false)
    # @type [Integer] The Stream ID.
    @stream_id = stream.getId
    # @type [Array<String>] The columns in the Dataset
    @dataset_columns = @domo_client.dataset_schema_columns(stream)
    # Get the dead letter queue writer if it's enabled.
    @dlq_writer = dlq_enabled? ? execution_context.dlq_writer : nil
    # If we're setting an automatic upload timestamp, make sure the field is actually in the dataset.
    if @upload_timestamp_field
      col_check = @dataset_columns.select { |col| col[:name] == @upload_timestamp_field }
      unless col_check.length > 0
        raise LogStash::ConfigurationError.new("The Upload Timestamp Field named #{@upload_timestamp_field} is not present in the Dataset's schema.")
      end
    end
    if @partition_field
      col_check = @dataset_columns.select { |col| col[:name] == @partition_field }
      unless col_check.length > 0
        raise LogStash::ConfigurationError.new("The Partition Field named #{@partition_field} is not present in the Dataset's schema.")
      end
    end

    # Distributed lock requires a redis queuing mechanism, among other things.
    if @distributed_lock
      if @redis_client.nil?
        raise LogStash::ConfigurationError.new("The redis_client parameter is required when using distributed_lock")
      else
        # Instantiate the redis client for the queue.
        @redis_client = symbolize_redis_client_args(@redis_client)
        unless @redis_sentinels.nil?
          @redis_client[:sentinels] = @redis_sentinels.map do |s|
            host, port = s.split(":")
            {
                :host => host,
                :port => port,
            }
          end
        end
        @redis_client = Redis.new(@redis_client)
      end

      if @lock_hosts.nil? or @lock_hosts.length <= 0
        raise LogStash::ConfigurationError.new("The lock_servers parameter is required when using distributed_lock")
      end

      redlock_options = {
          :retry_count => redlock_retry_count(@lock_retry_delay),
          :retry_delay => @lock_retry_delay,
      }
      @lock_manager = Redlock::Client.new(@lock_hosts, redlock_options)
    else
      @lock_manager = Domo::Queue::ThreadLockManager.new
    end

    @queue = get_queue
    # Reset the commit status in case of abnormal terminations
    unless @commit_thread&.status
      @lock_manager.lock(commit_lock_key, @lock_timeout) do |locked|
        if locked and @queue.commit_status == :running
          begin
            if @queue.execution_id
              stream_execution = @domo_client.stream_client.getExecution(@stream_id, @queue.execution_id)
              @queue.execution_id = nil unless stream_execution&.currentState == "ACTIVE"
            end
          rescue Java::ComDomoSdkRequest::RequestException => e
            status_code = Domo::Client.request_error_status_code(e)
            if status_code >= 400 and status_code < 500
              @queue.execution_id = nil
            else
              raise e
            end
          ensure
            @queue.commit_status = :open
          end
        end
      end
    end

    send_to_domo unless queue_processed?
    # Attempt a commit if there's an empty but active queue or start processing the queue if it already has jobs
    # This is a failsafe in case all workers on all servers stopped without committing
    if @queue.execution_id and queue_processed? and commit_ready?
      @commit_thread = Thread.new { commit_stream }
      commit_status = @commit_thread.value
    end
  end # def register

  # Sets up the appropriate queue based on our desired queue type (redis vs multi-threaded)
  #
  # @return [Domo::Queue::ThreadedQueue, Domo::Queue::Redis::JobQueue] The appropriate queue.
  def get_queue
    # Simple multi-threaded queue.
    return @queue if @queue.is_a? Domo::Queue::ThreadedQueue

    if @redis_client.nil?
      queue = Domo::Queue::ThreadedQueue.new(@dataset_id, @stream_id, pipeline_id)
    # Redis based queue.
    else
      # Attempt to load the queue from redis in case there are still active jobs.
      queue = Domo::Queue::Redis::JobQueue.active_queue(@redis_client, @dataset_id, @stream_id, pipeline_id)
      # If we can't find one, let's make one
      if queue.nil?
        begin
          queue = @lock_manager.lock!(lock_key, @lock_timeout) do |locked|
            Domo::Queue::Redis::JobQueue.new(@redis_client, @dataset_id, @stream_id, nil, pipeline_id)
          end
        rescue Redlock::LockError => e
          raise e if @queue.nil?
          return @queue
        end
      end
    end
    queue
  end

  public
  def multi_receive(events)
    # Get the current queue
    @queue = get_queue
    plugin_closing = false

    events.each do |event|
      if event == LogStash::SHUTDOWN
        plugin_closing = true
        break
      end

      # Encode the Event data and add a job to the queue
      begin
        data = encode_event_data(event)
        # Create a job for the event + data and add it to the queue.
        job = Domo::Queue::Job.new(event, data)
        @queue << job
      # Reject the invalid event and send it to the DLQ if it's enabled.
      rescue ColumnTypeError => e
        unless @dlq_writer.nil?
          @dlq_writer.write(event, e.log_entry)
        end
        @logger.error(e.log_entry,
                      :value       => e.val,
                      :column_name => e.col_name,
                      :data        => e.data,
                      :event       => event.to_hash,
                      :exception   => e)
      end
    end
    # Process the queue
    send_to_domo if @queue.length > 0
    @queue = get_queue
    # Commit
    if @commit_thread&.status == 'sleep'
      if commit_ready? or (@distributed_lock and plugin_closing)
        @commit_thread.run
        commit_status = @commit_thread.value
      end
      @queue.clear if commit_status == :success and @queue.is_a? Domo::Queue::ThreadedQueue
    elsif @commit_thread.nil? or !@commit_thread.status
      if commit_ready?
        commit_stream
      else
        @commit_thread = Thread.new { commit_stream }
      end
    end
  end
  
  public
  # Send Event data using the DOMO Streams API.
  def send_to_domo
    # Setup our failure queue.
    if @queue.is_a? Domo::Queue::RedisQueue
      failures = Domo::Queue::Redis::FailureQueue.from_job_queue!(@queue)
      sleep(0.1) until failures.processing_status == :processing
    else
      failures = Concurrent::Array.new
    end

    loop do
      @queue = get_queue
      sleep(0.1) until @queue.commit_status != :running

      break if @queue.length <= 0 and failures.length <= 0

      job = @queue.pop
      if job.nil?
        break if failures.length <= 0
        # Wait and retry failures if we're into that sort of thing.
        if @retry_failures or @distributed_lock
          # Clear out or update the Job's Execution ID if its associated Stream Execution is no longer valid.
          @lock_manager.lock(lock_key, @lock_timeout) do |locked|
            break unless locked

            @queue = get_queue
            unless @queue.execution_id.nil?
              stream_execution = @domo_client.stream_client.getExecution(@stream_id, @queue.execution_id)
              if not stream_execution.nil? and stream_execution.currentState == "ACTIVE"
                @queue.execution_id = stream_execution.getId
              else
                @queue.execution_id = nil
              end
            end
            if @queue.is_a? Domo::Queue::RedisQueue
              @queue = failures.reprocess_jobs!(@queue.execution_id)
              failures = Domo::Queue::Redis::FailureQueue.from_job_queue!(@queue)
              sleep(0.1) until failures.processing_status == :processing
            else
              @queue.reprocess_jobs!(failures, @queue.execution_id)
              failures.clear
            end
          end

          break if @queue.length <= 0 and failures.length <= 0
          sleep(@retry_delay)
        # Not retrying failures? I don't know why you aren't, but ok.
        else
          commit_stream
          return
        end
        # Restart the loop to process the failures.
        next
      end

      begin
        sleep(0.1) until @queue.commit_status != :running
        # Get or create a Stream Execution
        @lock_manager.lock(lock_key, @lock_timeout) do |locked|
          @queue = get_queue
          if locked
            stream_execution = nil
            # Check if the Queue's existing Stream Execution is still valid.
            unless @queue.execution_id.nil?
              stream_execution = @domo_client.stream_client.getExecution(@stream_id, @queue.execution_id)
              if stream_execution.currentState != "ACTIVE"
                @logger.error("Stream Exuection ID #{stream_execution.getId} for Stream ID #{@stream_id} is no longer active. Its current status is #{stream_execution.currentState}. A new Stream Execution will be created.",
                              :stream_id         => @stream_id,
                              :execution_id      => stream_execution.getId,
                              :queue_pipeline_id => pipeline_id,
                              :job               => job.to_hash(true),
                              :event             => job.event.to_hash,
                              :data              => job.data)
                stream_execution = nil
              end
            end
            # We didn't find an execution so let's make one.
            if stream_execution.nil?
              until @queue.commit_status != :wait and @queue.commit_status != :running
                sleep(0.5)
                if locked.is_a? Hash and locked[:validity] < 1000
                  locked = @lock_manager.lock(lock_key, @lock_timeout, extend: locked, extend_life: true)
                end
              end
              stream_execution = @domo_client.stream_client.createExecution(@stream_id)
              @queue.execution_id = stream_execution.getId
              @queue.part_num.set(0)
              @queue.commit_status = :open
            end
          end
        end

        # If the queue is missing an Execution ID (e.g. another worker committed the execution before we got here),
        # then it's time to defer this job to the next round of processing.
        if @queue.execution_id.nil?
          job.execution_id = nil
          @queue.unshift(job)
          next
        end

        # Ensure that the job has the right Execution ID and update its part number if we have to change the ID.
        if job.execution_id != @queue.execution_id
          job.execution_id = @queue.execution_id
          job.part_num = nil
        end
        if job.part_num.nil?
          job.part_num = @queue.part_num.incr
        end

        # Add a little jitter so Domo's API doesn't shit itself
        sleep(Random.new.rand(0.5))
        # Prevent a race condition when a long running commit is underway
        if @queue.commit_status != :wait and @queue.commit_status != :open
          failures << job
          next
        end
        # Upload the job's data
        @domo_client.stream_client.uploadDataPart(@stream_id, job.execution_id, job.part_num, job.data)
        # Debug log output
        execution_id = @queue.nil? ? job.execution_id : @queue.execution_id
        queue_pipeline_id = @queue.nil? ? pipeline_id : @queue.pipeline_id
        @logger.debug("Successfully wrote data to DOMO.",
                      :stream_id         => @stream_id,
                      :execution_id      => execution_id,
                      :queue_pipeline_id => queue_pipeline_id,
                      :job               => job.to_hash(true),
                      :event             => job.event.to_hash,
                      :data              => job.data)
      rescue Java::ComDomoSdkRequest::RequestException => e
        status_code = Domo::Client.request_error_status_code(e)
        if status_code.nil? or status_code == -1
          @logger.debug("We got a status code of -1 somehow. Let's look at the exception.",
                        :exception => e,
                        :status_code => status_code,
                        :message => e.to_s)
        end
        # DOMO sends back a 400 if a data part is uploaded to an Execution that's done committing. Hence the <= 400
        if status_code <= 400 or status_code >= 500 or status_code == 404
          # Queue the job to be retried if we're using the distributed lock or configured to retry.
          if @retry_failures or @distributed_lock
            @logger.info("Got a retriable error from the DOMO Streams API.",
                         :code              => e.getStatusCode,
                         :exception         => e,
                         :stream_id         => @stream_id,
                         :execution_id      => job.execution_id,
                         :job               => job.to_hash(true),
                         :event             => job.event.to_hash,
                         :data              => job.data)
            failures << job
            @logger.warn("Will sleep for #{@retry_delay} seconds before retrying requests.")
            sleep(@retry_delay)
          else
            @logger.error("Encountered a fatal error interacting with the DOMO Streams API.",
                          :code => e.getStatusCode,
                          :exception  => e,
                          :job        => job.to_hash(true),
                          :event      => job.event.to_hash,
                          :data       => job.data)
          end
          # Something really ain't right so let's give up and optionally write the event to the DLQ.
        else
          log_message = "Encountered a fatal error interacting with the DOMO Streams API."
          @logger.error(log_message,
                        :code       => e.getStatusCode,
                        :exception  => e,
                        :job        => job.to_hash(true),
                        :event      => job.event.to_hash,
                        :data       => job.data)
          unless @dlq_writer.nil?
            @dlq_writer.write(job.event, "#{log_message} Exception: #{e}")
          end
        end
      end
    end
  end

  public
  # Commit the active Stream Execution, and (re)set all the appropriate attributes on the queue.
  #
  # @param plugin_closing [Boolean] Indicates that Logstash is shutting down which may trigger an override of commit_delay
  # @return [Symbol] The status of the commit operation.
  def commit_stream(plugin_closing=false)
    commit_status = :failed
    # Convert commit_delay to ms
    lock_ttl = @commit_delay * 1000
    lock_ttl = @lock_timeout if lock_ttl < @lock_timeout

    @commit_lock = @lock_manager.lock(commit_lock_key, lock_ttl)
    return :wait unless @commit_lock or plugin_closing

    while @commit_lock or (plugin_closing and !@distributed_lock)
      # Don't commit unless we're ready or shutting down Logstash
      unless commit_ready? or plugin_closing
        # The amount of time to sleep before committing.
        sleep_time = (@queue.last_commit + @commit_delay) - Time.now.utc
        unless sleep_time <= 0
          @logger.debug("The API is not ready for committing yet. Will sleep for %0.2f seconds." % [sleep_time],
                        :stream_id    => @stream_id,
                        :dataset_id   => @dataset_id,
                        :execution_id => @queue.execution_id,
                        :commit_delay => @commit_delay,
                        :sleep_time   => sleep_time,
                        :last_commit  => @queue.last_commit,
                        :next_commit  => @queue.last_commit + sleep_time)
          # (Distributed lock only) Make sure we hold the lock for at least as long as the amount of time we're sleeping for.
          if @commit_lock.is_a? Hash and @commit_lock[:validity] <= sleep_time
            lock_timeout = sleep_time > @commit_delay * 1000 ? sleep_time + 1000 : @commit_delay * 1000
            @commit_lock = @lock_manager.lock(commit_lock_key, lock_timeout, extend: @commit_lock, extend_life: true)
          end
          @queue.commit_status = :open
          sleep(sleep_time)
        end
      end
      @commit_lock = @lock_manager.lock(commit_lock_key, lock_ttl, extend: @commit_lock, extend_life: true)
      # TODO: Hurry up and commit when there's a distributed lock.
      # Somebody else *should* grab the lock, but testing went poorly.
      sleep(0.1) until @queue.empty? #unless @distributed_lock
      # Acquire a lock on the key used for the non-commit API calls so nobody goes and creates a new Stream Execution in the middle of this.
      if @lock_manager.is_a? Domo::Queue::ThreadLockManager and @lock_manager.locks.include? lock_key
        api_lock = @lock_manager.locks[lock_key]
        if api_lock&.locked?
          if api_lock.try_lock
            api_lock.lock
          else
            api_lock = nil
          end
        else
          api_lock = @lock_manager.lock(lock_key, @lock_timeout)
        end
      else
        api_lock = @lock_manager.lock(lock_key, @lock_timeout)
      end
      return :wait unless api_lock

      if @commit_lock.is_a? Hash and @commit_lock[:validity] <= api_lock[:validity]
        @commit_lock = @lock_manager.lock(commit_lock_key, lock_ttl, extend: @commit_lock, extend_life: true)
      end
      # Validate the active Stream Execution
      return :open unless @queue.execution_id
      begin
        stream_execution = @domo_client.stream_client.getExecution(@stream_id, @queue.execution_id)
      rescue Java::ComDomoSdkRequest::RequestException => e
        status_code = Domo::Client.request_error_status_code(e)
        if status_code.nil? or status_code == -1
          @logger.debug("We got a status code of -1 somehow. Let's look at the exception.",
                        :exception => e,
                        :status_code => status_code,
                        :message => e.to_s)
        end
        # The Execution no longer exists.
        @lock_manager.unlock(api_lock) if api_lock
        @lock_manager.unlock(@commit_lock) if @commit_lock
        return :open if status_code == 404 or status_code == -1
        raise e
      end
      # Abort errored out streams
      if stream_execution.currentState == "ERROR" or stream_execution.currentState == "FAILED"
        @domo_client.stream_client.abortExecution(@stream_id, stream_execution.getId)
        @logger.error("Execution ID for #{stream_execution.getId} for Stream ID #{@stream_id} was aborted due to an error.",
                      :stream_id        => @stream_id,
                      :execution_id     => stream_execution.getId,
                      :execution_state  => stream_execution.currentState,
                      :execution        => stream_execution.to_s)
        @queue.commit_status = :failed
      # Commit!
      elsif stream_execution.currentState == "ACTIVE"
        execution_id = stream_execution.getId
        # Start the commit
        @queue.commit_status = :running
        @logger.debug("Beginning commit of Stream Execution #{execution_id} for Stream ID #{@stream_id}.",
                      :stream_id    => @stream_id,
                      :execution_id => execution_id,
                      :execution    => stream_execution.to_s)
        stream_execution = @domo_client.stream_client.commitExecution(@stream_id, execution_id)

        # Wait until the commit is actually done processing
        while stream_execution&.currentState == "ACTIVE"
          sleep(0.5)
          # Keep the locks active
          if @commit_lock.is_a? Hash and (@commit_lock[:validity] <= 1000 or api_lock[:validity] <= 1000)
            @commit_lock = @lock_manager.lock(commit_lock_key, lock_ttl, extend: @commit_lock, extend_life: true)
            api_lock = @lock_manager.lock(lock_key, @lock_timeout, extend: api_lock, extend_life: true)
          end
          # Update the StreamExecution from the API.
          begin
            stream_execution = @domo_client.stream_client.getExecution(@stream_id, execution_id)
          rescue Java::ComDomoSdkRequest::RequestException => e
            # Almost every exception means we're done.
            status_code = Domo::Client.request_error_status_code(e)
            if status_code.nil? or status_code == -1
              @logger.debug("We got a status code of -1 somehow. Let's look at the exception.",
                            :exception => e,
                            :status_code => status_code,
                            :message => e.to_s)
            end
            if status_code == 404 or status_code < 400 or status_code >= 500
              break
            else
              raise e
            end
          end
        end
        # Pause for API lag to condition race prevent :p
        sleep(0.5)
        # Mark the queue as committed.
        @queue.commit
        @logger.info("Committed Execution ID for #{execution_id} for Stream ID #{@stream_id}.",
                      :stream_id        => @stream_id,
                      :execution_id     => execution_id,
                      :execution        => stream_execution.to_s)
        @queue.commit_status = :success
      else
        @logger.warn("Stream Execution ID #{stream_execution.getId} for Stream ID #{@stream_id} could not be committed or aborted because its state is #{stream_execution.currentState}",
                     :stream_id        => @stream_id,
                     :execution_id     => stream_execution.getId,
                     :execution_state  => stream_execution.currentState,
                     :execution        => stream_execution.to_s)
        @queue.commit_status = :failed
      end
      # There is no execution to associate with the queue at this point.
      @queue.execution_id = nil unless @queue.commit_status == :success
      @lock_manager.unlock(api_lock) if api_lock
      break
    end
    if @commit_lock
      @commit_thread = nil
      @lock_manager.unlock(@commit_lock)
    end

    @queue.commit_status
  end

  public
  def close
    # This can happen somehow.
    if @queue.nil?
      return if @redis_client.nil?
      begin
        @queue = get_queue
        return if @queue.nil?
      rescue Redlock::LockError
        return
      end
    end
    send_to_domo if @queue.length > 0
    # Wake up the sleeping commit Thread, or make a new one to force fire off the commit
    if @commit_thread&.status == "sleep"
      @commit_thread.run
      commit_status = @commit_thread.value if @commit_thread
    elsif (@commit_thread.nil? or !@commit_thread.status) and @queue.execution_id
      commit_status = commit_stream(true)
    end
  end

  public
  # CSV encode event data to pass to DOMO
  #
  # @param event [LogStash::Event] The Event to be sent to DOMO.
  # @return [String] The CSV encoded string.
  def encode_event_data(event)
    column_names = @dataset_columns.map { |col| col[:name] }
    encode_options = {
        :headers => column_names,
        :write_headers => false,
        :return_headers => false,
    }

    csv_data = CSV.generate(String.new, encode_options) do |csv_obj|
      data = event.to_hash.flatten_with_path
      data = data.select { |k, _| column_names.include? k }
      discarded_fields = data.select { |k, _| !column_names.include? k }
      unless discarded_fields.nil? or discarded_fields.length <= 0
        @logger.warn("The event has fields that are not present in the Domo Dataset. They will be discarded.",
                     :fields => discarded_fields,
                     :event  => event)
      end

      @dataset_columns.each do |col|
        # Just extracting this so referencing it as a key in other hashes isn't so damn awkward to read
        col_name = col[:name]
        # Set the Upload timestamp if we're into that sort of thing.
        if @upload_timestamp_field and col_name == @upload_timestamp_field
          data[col_name] = Time.now.utc.to_datetime
        elsif @partition_field and col_name == @partition_field
          data[col_name] = Time.now.utc.to_date
        # Set the column value to null if it's missing from the event data
        elsif !data.has_key? col_name
          data[col_name] = nil
        end

        # Make sure the type matches what Domo expects.
        if @type_check
          unless data[col_name].nil? or ruby_domo_type_match?(data[col_name], col[:type])
            raise ColumnTypeError.new(col_name, col[:type], data[col_name].class, data[col_name], data)
          end
        end
      end

      data = data.sort_by { |k, _| column_names.index(k) }.to_h
      csv_obj << data.values
    end
    csv_data.strip
  end

  private
  # Let's the caller know if it's ok to fire a commit
  #
  # @return [Boolean]
  def commit_ready?
    return true if @commit_delay <= 0 or @queue.last_commit.nil?
    return true if @queue.last_commit + @commit_delay <= Time.now.utc
    false
  end

  private
  # Calculates an acceptable retry count for trying to acquiring a distributed lock.
  # The default values for the parameters match the defaults in the current version of redlock.
  #
  # @param retry_delay [Integer] The delay between retries (ms).
  # @param minimum_retry_count [Integer] The smallest number of allowable retries.
  # @return [Integer] The number of times redlock can retry acquiring a lock.
  def redlock_retry_count(retry_delay=200, minimum_retry_count=3)
    if @lock_timeout < 1000
      retry_count = @lock_timeout / retry_delay
    else
      retry_count = @lock_timeout / 1000 / retry_delay
    end
    return retry_count unless retry_count < minimum_retry_count
    minimum_retry_count
  end

  private
  # Takes a given value and returns a boolean indicating if its type matches the corresponding Domo column.
  #
  # @param val [Object] The object to inspect.
  # @param domo_column_type [Java::ComDomoSdkDatasetsModel::ColumnType] The Domo column type.
  # @return [Boolean] Whether or not the types match.
  def ruby_domo_type_match?(val, domo_column_type)
    case domo_column_type
    when Java::ComDomoSdkDatasetsModel::ColumnType::DATE
      return true if val.is_a? Date
      begin
        _ = Date.parse(val)
        return true
      rescue ArgumentError
        return false
      end
    when Java::ComDomoSdkDatasetsModel::ColumnType::DATETIME
      if val.is_a? LogStash::Timestamp or val.is_a? DateTime or val.is_a? Date or val.is_a? Time
        return true
      end
      begin
        _ = DateTime.parse(val)
        return true
      rescue ArgumentError
        return false
      end
    when Java::ComDomoSdkDatasetsModel::ColumnType::LONG
      if val.is_a? Integer
        return true
      end
      begin
        _ = Integer(val)
        return true
      rescue ArgumentError
        return false
      end
    when Java::ComDomoSdkDatasetsModel::ColumnType::DOUBLE
      if val.is_a? Float
        return true
      end
      begin
        _ = Float(val)
        return true
      rescue ArgumentError
        return false
      end
    else
      true
    end
  end

  private
  # Builds an array of Redis clients out of the plugin's configuration parameters for distributed locking
  #
  # @return [Array<Redis>]
  def lock_hosts(lock_hosts, lock_ports, lock_passwords)
    lock_servers = Array.new
    lock_hosts.each_with_index do |host, i|
      port = lock_ports.fetch(i, 6379)
      password = lock_passwords.fetch(i, nil)

      lock_servers << Redis.new("host" => host, "port" => port, "password" => password)
    end

    lock_servers
  end

  private
  # Convert all keys in the redis_client hash to symbols because Logstash makes them keys, but redis-rb wants symbols.
  #
  # @param redis_client_args [Hash]
  # @return [Hash]
  def symbolize_redis_client_args(redis_client_args)
    redis_client_args = redis_client_args.inject({}) {|memo, (k, v)| memo[k.to_sym] = v; memo}
    unless redis_client_args.fetch(:sentinels, nil).nil?

      redis_client_args[:sentinels] = redis_client_args[:sentinels].map do |sentinel|
        sentinel.inject({}) {|memo, (k, v)| memo[k.to_sym] = v; memo}
      end
    end

    redis_client_args
  end

  private
  # Checks if the Dead Letter Queue is enabled
  #
  # @return [Boolean]
  def dlq_enabled?
    # Thanks Elasticsearch plugin team!
    # https://github.com/logstash-plugins/logstash-output-elasticsearch/blob/master/lib/logstash/outputs/elasticsearch/common.rb#L349
    # See more in: https://github.com/elastic/logstash/issues/8064
    respond_to?(:execution_context) && execution_context.respond_to?(:dlq_writer) &&
        !execution_context.dlq_writer.inner_writer.is_a?(::LogStash::Util::DummyDeadLetterQueueWriter)
  end

  private
  # Raised when an event field has a type mismatch with a Domo Dataset Column
  class ColumnTypeError < TypeError
    # @return [String]
    attr_reader :col_name
    # @return [Class]
    attr_reader :expected_type
    # @return [Class]
    attr_reader :actual_type
    # @return [Object]
    attr_reader :val
    # @return [Object]
    attr_reader :data

    # A human readable message for error log entries
    # @return [String]
    def log_entry
      "#{@actual_type} is an invalid type for #{@col_name} with the value #{@val}. It should be #{@expected_type}"
    end

    # @param col_name [String]
    # @param expected_type [Class]
    # @param actual_type [Class]
    # @param val [Object]
    # @param data [Hash, nil]
    def initialize(col_name, expected_type, actual_type, val=nil, data=nil)
      @col_name = col_name
      @expected_type = expected_type
      @actual_type = actual_type
      @val = val
      @data = data

      super(log_entry)
    end
  end
end # class LogStash::Outputs::Domo
