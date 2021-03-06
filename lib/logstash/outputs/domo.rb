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

  # The minimum number of rows before a Job is processed.
  # Set to 0 (default) to disable.
  config :upload_min_batch_size, :validate => :number, :default => 0

  # The maximum number of rows in a single Data Part.
  # Set to 0 (default) to disable
  config :upload_max_batch_size, :validate => :number, :default => 0

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
  #   "rediss://:password@host-tls:port/db"
  # ]
  # -----------------------
  #
  # NOTE: Currently, if using rediss, then the plugin will *NOT* verify peer certificates.
  # We'll happily accept pull requests to add support for peer certificate verification.
  config :lock_hosts, :validate => :array

  # A hash with connection information for the redis client for the Queue
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
  # Below is a variation that uses redis+tls.
  # NOTE: If you need to verify certs, you are responsible for figuring out key and cert distribution.
  # The ssl_params hash will take any argument that OpenSSL::SSL::SSLContext finds acceptable
  # https://ruby-doc.org/stdlib-2.3.0/libdoc/openssl/rdoc/OpenSSL/SSL/SSLContext.html
  # [source,ruby]
  # ----------------------------------
  # redis_client => {
  #   "url"        => "rediss://:password@host/db"
  #   "ssl_params" => {
  #     "verify_mode" => "VERIFY_NONE"
  #   }
  # }
  # -----------------------
  #
  # The documentation for the Redis class's constructor can be found at the following URL:
  # https://www.rubydoc.info/gems/redis/Redis#initialize-instance_method
  config :redis_client, :validate => :hash, :required => true

  # Optional redis sentinels to associate with redis_client.
  # NOTE: Your results will be *very* unpredictable if you combine this with a redis+tls connection.
  #
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
    "logstash-output-domo:#{@dataset_id}:#{@stream_id}_lock"
  end

  # @!attribute [r] pending_lock_key
  # The name of the redis key for locking the pending queue.
  # @return [String]
  def pending_lock_key
    "logstash-output-domo:#{@dataset_id}:#{@stream_id}_pending_lock"
  end

  # @!attribute [r] commit_lock_key
  # The name of the redis key for locking the current commit.
  # @return [String]
  def commit_lock_key
    "logstash-output-domo:#{@dataset_id}:#{@stream_id}_commit_lock"
  end

  # @!attribute [r] part_num_key
  # The redis key for getting / incrementing the part number.
  # @return [String]
  def part_num_key
    return @queue.part_num_key if @queue.is_a? Domo::Queue::RedisQueue

    part_num_key = "#{Domo::Queue::RedisQueue::KEY_PREFIX_FORMAT}" % {:dataset_id => @dataset_id, :stream_id => @stream_id}
    "#{part_num_key}#{Domo::Queue::RedisQueue::KEY_SUFFIXES[:PART_NUM]}"
  end

  public
  def register
    # @type [Domo::Client] A client connection to Domo's APIs.
    @domo_client = Domo::Client.new(@client_id, @client_secret, @api_host, @api_ssl, Java::ComDomoSdkRequest::Scope::DATA)

    # @type [Domo::Queue::ThreadLockManager] The Mutex we'll be using to lock writes to @commit_thread
    @semaphore = Domo::Queue::ThreadLockManager.new

    if @upload_max_batch_size > 0 and @upload_min_batch_size > @upload_max_batch_size
      raise LogStash::ConfigurationError.new("upload_min_batch_size cannot be larger than upload_max_batch_size")
    end

    # Validate the Dataset and Stream
    begin
      dataset = @domo_client.dataset(@dataset_id)
      # @type [Java::ComDomoSdkStreamModel::Stream] The Domo Stream associated with our Stream ID and/or Dataset.
      stream = @domo_client.stream(@stream_id, @dataset_id, false)
    rescue Java::ComDomoSdkRequest::RequestException => e
      status_code = Domo::Client.request_error_status_code(e)
      @logger.error(e, :status_code => status_code, :dataset_id => @dataset_id, :stream_id => @stream_id)
      raise LogStash::ConfigurationError, "Either Dataset ID #{@dataset_id} is invalid or the Stream (#{@stream_id}) associated with it is invalid. See error logs for more detail."
    end
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
    # If we're setting a Partition ID, make sure the field is actually in the Dataset.
    if @partition_field
      col_check = @dataset_columns.select { |col| col[:name] == @partition_field }
      unless col_check.length > 0
        raise LogStash::ConfigurationError.new("The Partition Field named #{@partition_field} is not present in the Dataset's schema.")
      end
    end
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
    # Distributed lock requires a redis queuing mechanism, among other things.
    if @distributed_lock
      if @lock_hosts.nil? or @lock_hosts.length <= 0
        raise LogStash::ConfigurationError.new("The lock_servers parameter is required when using distributed_lock")
      end

      redlock_options = {
          :retry_count => redlock_retry_count(@lock_retry_delay),
          :retry_delay => @lock_retry_delay,
      }
      # Add redis+tls crap if need be.
      @lock_hosts = lock_hosts_tls_opts(@lock_hosts)
      @commit_lock_manager = Redlock::Client.new(@lock_hosts, redlock_options)
    else
      @commit_lock_manager = Domo::Queue::ThreadLockManager.new
    end

    @queue = get_queue
    # @type [Concurrent::ScheduledTask, nil]
    @semaphore.lock(lock_key, @lock_timeout) do |locked|
      @commit_task ||= nil
    end
    # Reset the commit status and possibly the Queue's execution_id after possible abnormal terminations
    @commit_lock_manager.lock(commit_lock_key, @lock_timeout) do |locked|
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
          @queue.processing_status = :open if @queue.execution_id.nil?
          @queue.commit_status = :open
        end
      end
    end
    # Start processing the queue if it already has jobs
    # This is a failsafe in case all workers on all servers stopped without committing
    unless @queue.processed?
      # Upload any jobs that are already in the queue.
      send_to_domo until @queue.processed?
      # Commit if we're ready
      try_commit
    end
  end # def register

  # Get the active queue
  #
  # @return [Domo::Queue::Redis::JobQueue]
  def get_queue
    return @queue unless @queue.nil?

    Domo::Queue::Redis::JobQueue.active_queue(@redis_client, @dataset_id, @stream_id, pipeline_id)
  end

  public
  def multi_receive(events)
    # Get the current queue
    @queue = get_queue
    # LogStash::SHUTDOWN events will set this to true
    shutdown = false
    # The Array of CSV strings to associate with upload job data.
    data = Array.new
    # When using upload_max_batch_size we keep track of the number of batches generated from the provided events
    num_batches = 1
    # Loop through the incoming events, encode them, add the encoded data to the data Array, and parcel out jobs.
    events.each_with_index do |event, i|
      if event == LogStash::SHUTDOWN
        shutdown = true
        break
      end
      if event.is_a? LogStash::Event
        # Encode the Event data
        begin
          encoded_event = encode_event_data(event)
          data << encoded_event
        # Reject the invalid event and send it to the DLQ if it's enabled.
        rescue ColumnTypeError => e
          @dlq_writer.write(event, e.log_entry) unless @dlq_writer.nil?
          @logger.error(e.log_entry,
                        :value       => e.val,
                        :column_name => e.col_name,
                        :event       => event.to_hash,
                        :exception   => e)
        end
      # Spamming events seems to get us to this point during/after restarts (on dev at least)
      elsif event.is_a? Array
        data += event.map do |e|
          if e.is_a? LogStash::Event
            begin
              encode_event_data(e)
            rescue ColumnTypeError => ex
              @dlq_writer.write(e, ex.log_entry) unless @dlq_writer.nil?
              @logger.error(ex.log_entry,
                            :value       => ex.val,
                            :column_name => ex.col_name,
                            :event       => e.to_hash,
                            :exception   => ex)
            end
          elsif e.is_a? String
            e if e.split(',').length == @dataset_columns.length
          end
        end
      end
      # Carve out Job batches if we have a maximum batch size.
      if @upload_max_batch_size > 0 and data.length + 1 >= @upload_max_batch_size * num_batches
        job = Domo::Queue::Job.new(data, @upload_min_batch_size)
        @queue << job
        data.clear
        num_batches += 1
      end
    end
    # Merge pending jobs with our data
    data = merge_pending_jobs!(data, shutdown)
    # Only make a job if we have data
    unless data.length <= 0
      job = Domo::Queue::Job.new(data, @upload_min_batch_size)
      # Put incomplete jobs in the pending queue
      if job.status == :incomplete and !shutdown
        @queue.pending_jobs << job.data
        @logger.trace("Putting job in pending queue",
                      :stream_id         => @stream_id,
                      :execution_id      => @queue.execution_id,
                      :pipeline_id       => pipeline_id,
                      :rows_pending      => @queue.pending_jobs.length,
                      :job               => job.to_hash(true))
      # Everybody else goes to the main queue
      else
        @queue << job
        @logger.info("Added job to the main queue",
                      :stream_id         => @stream_id,
                      :execution_id      => @queue.execution_id,
                      :pipeline_id       => pipeline_id,
                      :job               => job.to_hash(true))
      end
    end
    # If the queue is empty, we're not shutting down, we're not ready to commit, and we have no data to add, give up
    if data.nil? or data.length <= 0
      return unless shutdown or @queue.unprocessed?(shutdown)
    end
    # Process the queue
    send_to_domo(shutdown) until @queue.processed?(shutdown)
    # Commit if we're ready
    try_commit(shutdown)
  end

  public
  def try_commit(shutdown=false)
    if commit_ready? and @queue.execution_id
      commit_stream(shutdown) unless @queue.commit_scheduled?

      if @queue.commit_stalled?(@commit_delay) and @queue.commit_scheduled?
        @semaphore.lock(lock_key) do |locked|
          @commit_task.cancel if locked and @commit_task&.pending?
        end
        commit_stream(shutdown)
      end
    elsif shutdown
      @semaphore.lock(lock_key) do |locked|
        if locked
          if @commit_task&.processing?
            sleep(0.1) while @commit_task&.processing?
          else
            @commit_task.cancel unless @commit_task.nil? or @commit_task.complete?
          end
        end
      end

      commit_stream(shutdown) unless @queue.processed?(true) and @queue.execution_id.nil?
    else
      # The amount of time to sleep before committing.
      sleep_time = @queue.commit_delay(@commit_delay)
      if sleep_time > 0 and @queue.commit_status != :running
        @queue.commit_status = :open

        thread_locked = @semaphore.lock(lock_key)
        return unless thread_locked

        if @queue.commit_stalled?(@commit_delay)
          if @commit_task.nil? and @queue.execution_id
            @commit_task = Concurrent::ScheduledTask.execute(0.0) { commit_stream(shutdown) }
          elsif @commit_task&.pending?
            @commit_task.reschedule(sleep_time)
            @queue.schedule
          end

          sleep(0.1) until @commit_task.complete? if @commit_task&.processing?

          if @commit_task&.incomplete? and @queue.execution_id
            if thread_locked
              @commit_task.cancel
              @commit_task = Concurrent::ScheduledTask.execute(sleep_time) { commit_stream(shutdown) }
            end
          end
        end

        if @queue.commit_unscheduled? and @commit_task&.pending?
          scheduled_time = Time.at(@commit_task.schedule_time).utc
          @queue.schedule(scheduled_time - @commit_delay)
        end
        if @queue.commit_unscheduled? and @queue.execution_id
          begin
            @commit_lock_manager.lock(commit_lock_key, @lock_timeout) do |locked|
              if locked and @queue.commit_unscheduled?
                @queue.schedule
                @logger.info("The API is not ready for committing yet. Will sleep for %0.2f seconds." % [sleep_time],
                             :stream_id    => @stream_id,
                             :pipeline_id  => pipeline_id,
                             :dataset_id   => @dataset_id,
                             :execution_id => @queue.execution_id,
                             :commit_delay => @commit_delay,
                             :sleep_time   => sleep_time,
                             :last_commit  => @queue.last_commit,
                             :next_commit  => @queue.next_commit(@commit_delay),
                             :commit_rows  => @queue.commit_rows)
                @semaphore.unlock(thread_locked)
                @commit_task = Concurrent::ScheduledTask.execute(sleep_time) { commit_stream(shutdown) }
              else
                @semaphore.unlock(thread_locked) if thread_locked
              end
            end
          rescue Redis::BaseConnectionError
            # Once redlock-rb does a release with this pull request we can drop this rescue block https://github.com/leandromoreira/redlock-rb/pull/75
            @semaphore.unlock(thread_locked) if thread_locked
          end
        elsif @commit_task&.pending?
          @commit_task.reschedule(sleep_time)
          @queue.schedule
          @semaphore.unlock(thread_locked) if thread_locked
        end
      else
        commit_stream(shutdown)
      end
    end
  end
  
  public
  # Send Event data using the DOMO Streams API.
  #
  # @param force_pending [Boolean] Force process all pending jobs.
  # @param force_run [Boolean] Ignore the commit status of the queue.
  def send_to_domo(force_pending=false, force_run=false)
    @queue = get_queue
    return if @queue.processed?(force_pending) and !force_run

    # Block until failed jobs are done reprocessing back into the queue
    sleep(0.1) until @queue.failures.length <= 0 or @queue.failures.processing_status != :reprocessing
    # Process the queue
    loop do
      # Update on each pass in case another thread managed to change things in a thread unsafe way
      @queue = get_queue
      # Merge pending jobs into one job and add it to the main queue if we're force processing pending jobs.
      if force_pending and @queue.pending_jobs.length > 0
        data = merge_pending_jobs!(Array.new, force_pending)
        # Prevent infinite loops
        force_pending = false
        @queue << Domo::Queue::Job.new(data, 0) unless data.length <= 0
        # Restart the loop
        next
      end

      # Block the loop while commits are in progress
      # But also timeout after an hour to keep things moving during a likely failure
      until force_run or @queue.commit_status != :running or @queue.stuck?(3600)
        sleep(0.1)
        next unless @queue.execution_id
      end

      break if @queue.processed?(force_pending)

      # Get the job from the queue
      job = @queue.pop
      # Skip the job if it has no data for some reason
      unless job.nil? or job.row_count > 0
        @queue.processing_status = :open
        next
      end
      # If we're out of jobs, process any failures
      if job.nil?
        break if @queue.failures.length <= 0

        # Retry failures if we're into that sort of thing.
        if @retry_failures or @distributed_lock
          # Clear out or update the Job's Execution ID if its associated Stream Execution is no longer valid.
          @commit_lock_manager.lock(lock_key, @lock_timeout) do |locked|
            break unless locked

            # Validate the queue's StreamExecution
            unless @queue.execution_id.nil?
              begin
                stream_execution = @domo_client.stream_client.getExecution(@stream_id, @queue.execution_id)
                if not stream_execution.nil? and stream_execution.currentState == "ACTIVE"
                  @queue.execution_id = stream_execution.getId
                else
                  @queue.execution_id = nil
                end
              rescue Java::ComDomoSdkRequest::RequestException =>ex
                # HTTP status code from the API request
                status_code = Domo::Client.request_error_status_code(ex)
                # Clear the Queue's Execution ID if we got a 404
                @queue.execution_id = nil if status_code == 404
                # Other status code 4xx errors should be raised
                raise(ex) if status_code != 404 and status_code >= 400 and status_code < 500
                # Retry for 5xx or unknown errors
                sleep(@retry_delay) if status_code < 100 or status_code >= 500
                # Regardless, restart the loop
                next
              end
            end
            # Block if a commit started during the validation
            sleep(0.1) until @queue.commit_status != :running or @queue.stuck?(3600)
            # Reprocess the failed jobs
            @queue = @queue.failures.reprocess_jobs!
            # Block if another thread is reprocessing the jobs
            sleep(0.1) until @queue.failures.processing_status != :reprocessing
          end

          break if @queue.processed?
        # Not retrying failures? I don't know why you aren't, but ok.
        else
          commit_stream
          return
        end
        # Restart the loop to process the failures.
        next
      end
      # Process the queued job and upload its data to Domo.
      begin
        # Block during commits
        sleep(0.1) until force_run or @queue.commit_status != :running or @queue.stuck?(3600)
        # Prevent unnecessary executions
        return if @queue.processed? and @queue.commit_complete?
        # Get or create a Stream Execution
        @commit_lock_manager.lock(lock_key, @lock_timeout) do |locked|
          if locked
            stream_execution = nil
            # Check if the Queue's existing Stream Execution is still valid.
            unless @queue.execution_id.nil?
              stream_execution = @domo_client.stream_client.getExecution(@stream_id, @queue.execution_id)
              if stream_execution.currentState != "ACTIVE"
                @logger.error("Stream Exuection ID #{stream_execution.getId} for Stream ID #{@stream_id} is no longer active. Its current status is #{stream_execution.currentState}. A new Stream Execution will be created.",
                              :stream_id         => @stream_id,
                              :execution_id      => stream_execution.getId,
                              :pipeline_id       => pipeline_id,
                              :job               => job.to_hash(true),
                              :data              => job.data)
                stream_execution = nil
              end
            end
            # We didn't find a valid execution so let's make a new one.
            if stream_execution.nil?
              # If we're forcing processing and there's no Execution ID, we need to just return and let #commit_stream figure this mess out
              if force_run
                @queue.failures << job
                return
              end
              until @queue.commit_status != :wait and @queue.commit_status != :running
                sleep(0.5)
                if locked.is_a? Hash and locked[:validity] < 1000
                  locked = @commit_lock_manager.lock(lock_key, @lock_timeout, extend: locked, extend_only_if_locked: true)
                end
              end
              stream_execution = @domo_client.stream_client.createExecution(@stream_id)
              @queue.execution_id = stream_execution.getId
              @queue.commit_status = :open
            end
          end
        end
        # If the queue is missing an Execution ID (e.g. another worker committed the execution before we got here),
        # then it's time to defer this job to the next round of processing.
        if @queue.execution_id.nil? or @queue.commit_status == :running
          job.data_part = nil
          @queue.unshift(job)
          next
        end
        # Block until failed jobs are done reprocessing back into the queue
        sleep(0.1) until @queue.upload_ready?
        # Prevent a race condition when a long running commit is underway
        unless @queue.commit_status == :open
          job.data_part = job.data_part&.execution_id == @queue.execution_id ? job.data_part : nil
          @queue.unshift(job)
          next
        end
        # Reset and/or increment the job's data part if need be
        job.data_part = @queue.incr_data_part if job.data_part.nil? or job.data_part.execution_id != @queue.execution_id
        # Upload the job's data to Domo.
        @domo_client.stream_client.uploadDataPart(@stream_id, job.data_part.execution_id, job.data_part.part_id, job.upload_data)
        @queue.add_commit_rows(job.row_count)
        # Debug log output
        execution_id = @queue.nil? ? job.execution_id : @queue.execution_id
        queue_pipeline_id = @queue.nil? ? pipeline_id : @queue.pipeline_id
        @logger.info("Successfully wrote data to DOMO.",
                      :stream_id         => @stream_id,
                      :execution_id      => execution_id,
                      :queue_pipeline_id => queue_pipeline_id,
                      :commit_rows       => @queue&.commit_rows,
                      :job               => job.to_hash(true))
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
                         :job               => job.to_hash(true))
            @queue.failures << job
            @logger.warn("Will sleep for #{@retry_delay} seconds before retrying requests.")
            sleep(@retry_delay)
          else
            @logger.error("Encountered a fatal error interacting with the DOMO Streams API.",
                          :code => e.getStatusCode,
                          :exception  => e,
                          :job        => job.to_hash(true))
          end
        # Something really ain't right.
        # We used to write to the DLQ in this scenario, but,
        # since we're batch processing now, we'll have to just add it to the failures queue and pray.
        else
          log_message = "Encountered a fatal error interacting with the DOMO Streams API."
          @logger.error(log_message,
                        :code       => e.getStatusCode,
                        :exception  => e,
                        :job        => job.to_hash(true))
          @queue.failures << job unless @queue.nil?
        end
      ensure
        @queue = get_queue if @queue.nil?
        @queue.processing_status = :open
      end
    end
  end

  public
  # Commit the active Stream Execution, and (re)set all the appropriate attributes on the queue.
  #
  # @param shutdown [Boolean] Indicates that Logstash is shutting down which will force processing all jobs and commit
  # @return [Symbol] The status of the commit operation.
  def commit_stream(shutdown=false)
    @queue = get_queue
    @queue.run # Notify other workers that the commit queue is open for business again
    # No point in continuing if we're not ready
    return :wait unless commit_ready? or shutdown
    # Convert commit_delay to ms
    lock_ttl = @commit_delay * 1000
    lock_ttl = @lock_timeout if lock_ttl < @lock_timeout
    commit_lock = false
    # Acquire a lock to prevent other workers from executing this function.
    @semaphore.lock(commit_lock_key) do |locked|
      return :wait unless locked
      # Hopefully, redlock-rb will accept our pull request so we don't need this rescue block anymore
      begin
        commit_lock = @commit_lock_manager.lock(commit_lock_key, lock_ttl)
      rescue Redis::BaseConnectionError
        return :wait
      end
      # We'll be locking API later on
      api_lock = false
      return :wait unless commit_lock

      begin
        # Block everybody from uploading now
        @queue.commit_status = :running
        # Keep hanging on to the commit lock until we can commit.
        while commit_lock
          # Don't commit unless we're ready or shutting down Logstash
          # Grab the lock again if we managed to lose it while sleeping
          unless commit_lock
            commit_lock = @commit_lock_manager.lock(commit_lock_key, lock_ttl)
          end
          break unless commit_lock
          # Clear out the queue
          send_to_domo(shutdown, true) until @queue.processed?(shutdown)
          # Acquire a lock on the key used for the non-commit API calls so nobody goes and creates a new Stream Execution in the middle of this.
          api_lock = @commit_lock_manager.lock(lock_key, @lock_timeout)
          break unless api_lock and commit_lock
          # Make sure the API lock and commit lock will last for at least the same amount of time.
          if commit_lock[:validity] <= api_lock[:validity]
            commit_lock = @commit_lock_manager.lock(commit_lock_key, lock_ttl, extend: commit_lock, extend_only_if_locked: true)
          end

          # Validate the active Stream Execution
          unless @queue.execution_id
            @queue.commit_status = :open
            break
          end
          # Do one last validation on the Stream Execution and abort if there are irregularities.
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
            if status_code == 404 or status_code == -1
              @queue.commit_status = :open
              @commit_lock_manager.unlock(api_lock) if api_lock
              @commit_lock_manager.unlock(commit_lock) if commit_lock
              return :open
            else
              @commit_lock_manager.unlock(api_lock) if api_lock
              @commit_lock_manager.unlock(commit_lock) if commit_lock
              raise e
            end
          end
          # Abort errored out streams
          if stream_execution.currentState == "ERROR" or stream_execution.currentState == "FAILED"
            @domo_client.stream_client.abortExecution(@stream_id, stream_execution.getId)
            @logger.error("Execution ID for #{stream_execution.getId} for Stream ID #{@stream_id} was aborted due to an error.",
                          :stream_id        => @stream_id,
                          :dataset_id       => @dataset_id,
                          :execution_id     => stream_execution.getId,
                          :execution_state  => stream_execution.currentState,
                          :execution        => stream_execution.to_s)
            @queue.commit_status = :failed
          # Commit!
          elsif stream_execution.currentState == "ACTIVE"
            execution_id = stream_execution.getId
            # Block everybody from uploading again
            @queue.commit_status = :running
            sleep(0.5) # Race conditions are a PITA
            # Start the commit
            @logger.info("Beginning commit of Stream Execution #{execution_id} for Stream ID #{@stream_id}.",
                          :stream_id    => @stream_id,
                          :pipeline_id  => pipeline_id,
                          :dataset_id   => @dataset_id,
                          :execution_id => execution_id,
                          :commit_rows  => @queue.commit_rows,
                          :execution    => stream_execution.to_s)
            stream_execution = Concurrent::Future.execute { @domo_client.stream_client.commitExecution(@stream_id, execution_id) }
            until stream_execution.complete?
              sleep(0.5)
              # Keep the locks active
              if commit_lock[:validity] <= 1000 or api_lock[:validity] <= 1000
                commit_lock = @commit_lock_manager.lock(commit_lock_key, lock_ttl, extend: commit_lock, extend_only_if_locked: true)
                api_lock = @commit_lock_manager.lock(lock_key, @lock_timeout, extend: api_lock, extend_only_if_locked: true)
              end
            end

            stream_execution = stream_execution.value
            # Wait until the commit is actually done processing
            while stream_execution&.currentState == "ACTIVE"
              sleep(0.5)
              # Attempt to grab the lock again if we lost it
              commit_lock = @commit_lock_manager.lock(commit_lock_key, lock_ttl) unless commit_lock
              # Give up if we still can't get it
              return :wait unless commit_lock
              # Keep the locks active
              commit_lock = @commit_lock_manager.lock(commit_lock_key, lock_ttl, extend: commit_lock, extend_only_if_locked: true) if commit_lock[:validity] <= 1000
              api_lock = @commit_lock_manager.lock(lock_key, @lock_timeout, extend: api_lock, extend_only_if_locked: true) if api_lock and api_lock[:validity] <= 1000
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
            # Mark the queue as successfully committed.
            commit_rows = @queue.commit_rows
            @queue.commit
            @logger.info("Committed Execution ID for #{execution_id} for Stream ID #{@stream_id}.",
                          :stream_id        => @stream_id,
                          :pipeline_id      => pipeline_id,
                          :dataset_id       => @dataset_id,
                          :execution_id     => execution_id,
                          :commit_rows      => commit_rows,
                          :execution        => stream_execution.to_s)
          else
            @logger.warn("Stream Execution ID #{stream_execution.getId} for Stream ID #{@stream_id} could not be committed or aborted because its state is #{stream_execution.currentState}",
                         :stream_id        => @stream_id,
                         :pipeline_id      => pipeline_id,
                         :dataset_id       => @dataset_id,
                         :commit_rows      => @queue.commit_rows,
                         :execution_id     => stream_execution.getId,
                         :execution_state  => stream_execution.currentState,
                         :execution        => stream_execution.to_s)
            @queue.commit_status = :failed
          end
          break
        end
      # Make sure to unlock all the locks
      ensure
        @commit_lock_manager.unlock(api_lock) if api_lock
        @commit_lock_manager.unlock(commit_lock) if commit_lock
        # Open the queue back up if the execution failed
        @queue.commit_status = :open if @queue.commit_status == :running
      end
    end
    # Return the status of the commit.
    @queue.commit_status
  end

  public
  def close
    @queue = get_queue
    send_to_domo until @queue.processed?
    try_commit(true) if commit_ready?
    # Commit if we're ready
    # if commit_ready? and (@queue.commit_unscheduled? or @queue.commit_stalled?(@commit_delay))
    #   if @commit_task.nil? or @commit_task.complete?
    #     commit_status = commit_stream(true)
    #     return
    #   end
    #   unless @commit_task&.processing?
    #     @commit_task.cancel
    #     commit_status = commit_stream(true)
    #     return
    #   end
    #   sleep(0.1) while @commit_task&.processing?
    #   commit_status = @commit_task&.value
    # end
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

  public
  # Let's the caller know if it's ok to fire a commit
  #
  # @return [Boolean]
  def commit_ready?
    queue = get_queue
    return false unless queue&.execution_id and queue.commit_rows > 0

    if queue.processed? and queue.commit_status != :running
      return true if @commit_delay <= 0 or queue.last_commit.nil?
      return true if queue.next_commit(@commit_delay) <= Time.now.utc
    end
    false
  end

  private
  # Merge the data in all of our pending jobs
  # If we're shutting down or the size of the merged data exceeds upload_min_batch_size, then the data will be returned.
  # Otherwise, the new job will be thrown back into the pending queue until it is ready, and nil will be returned.
  #
  # @param data [Array<String>] The CSV data to upload to DOMO.
  # @param shutdown [Boolean] Indicates that Logstash is shutting down.
  # @return [Array, nil]
  def merge_pending_jobs!(data, shutdown=false)
    merged_data = data
    rows_start = data.length
    begin
      # Grab a lock so other workers don't fight over grabbing these jobs
      @commit_lock_manager.lock(pending_lock_key, @lock_timeout*2) do |locked|
        return merged_data unless locked

        while @queue.pending_jobs.merge_ready?(rows_start, @upload_min_batch_size, shutdown)
          merged_data = @queue.pending_jobs.reduce(merged_data, @upload_min_batch_size, shutdown)
          break if merged_data.length == rows_start

        end
      end
      if merged_data.length > rows_start
        @logger.debug("Merged pending jobs",
                      :stream_id         => @stream_id,
                      :execution_id      => @queue.execution_id,
                      :pipeline_id       => pipeline_id,
                      :rows_start        => rows_start,
                      :rows_pending      => @queue.pending_jobs.length,
                      :rows_merged       => merged_data.length)
      end
    rescue Redlock::LockError
      # Do nothing
    rescue Redis::BaseConnectionError
      # Do nothing
    end

    merged_data
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
  # Figure out which OpenSSL::SSL::VERIFY constant matches with a string on the config.
  #
  # @param val [String]
  # @return [Integer, String]
  def openssl_opt_from_string(val)
    case val
    when 'VERIFY_NONE'
      OpenSSL::SSL::VERIFY_NONE
    when 'VERIFY_PEER'
      OpenSSL::SSL::VERIFY_PEER
    when 'VERIFY_FAIL_IF_NO_PEER_CERT'
      OpenSSL::SSL::VERIFY_FAIL_IF_NO_PEER_CERT
    when 'VERIFY_CLIENT_ONCE'
      OpenSSL::SSL::VERIFY_CLIENT_ONCE
    else
      val
    end
  end

  private
  # Symbol all keys in a hash, as well as keys from embedded hashes.
  #
  # @param obj [Hash]
  # @return [Hash]
  def recursive_symbolize(obj)
    obj.reduce({}) do |memo, (k, v)|
      if v.is_a? Hash
        memo[k.to_sym] = recursive_symbolize(v)
      else
        memo[k.to_sym] = openssl_opt_from_string(v)
      end
      memo
    end
  end

  private
  # Loop through lock_hosts from the configuration and add TLS options if they're using rediss
  #
  # @param lock_hosts [Hash]
  # @return [Array<Redis>]
  def lock_hosts_tls_opts(lock_hosts)
    lock_hosts.map do |host|
      if host.start_with?('rediss')
        Redis.new(url: host, ssl_params: {:verify_mode => OpenSSL::SSL::VERIFY_NONE})
      else
        Redis.new(url: host)
      end
    end
  end

  private
  # Convert all keys in the redis_client hash to symbols because Logstash makes them keys, but redis-rb wants symbols.
  #
  # @param redis_client_args [Hash]
  # @return [Hash]
  def symbolize_redis_client_args(redis_client_args)
    # redis_client_args = redis_client_args.inject({}) {|memo, (k, v)| memo[k.to_sym] = v; memo}
    redis_client_args = recursive_symbolize(redis_client_args)
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
