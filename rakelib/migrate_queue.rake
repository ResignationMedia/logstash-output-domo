require "java"
java_import "com.domo.sdk.streams.model.Stream"

def init_redis_client
  redis_client = {:url => ENV["REDIS_URL"]}
  redis_client[:sentinels] = ENV.select { |k, v| k.start_with? "REDIS_SENTINEL_HOST"}.map do |k, v|
    index = k.split("_")[-1].to_i
    port = ENV.fetch("REDIS_SENTINEL_PORT_#{index}", 26379)

    {
        :host => v,
        :port => port,
    }
  end

  if redis_client[:sentinels].length <= 0
    redis_client = redis_client.reject { |k, v| k == :sentinels }
  end

  Redis.new(redis_client)
end

def validate_settings!(settings, args)
  raise KeyError, 'domo' unless settings.has_key?('domo')
  raise ArgumentError, 'The old_dataset_id argument is required' if args.old_dataset_id.nil?
  raise ArgumentError, 'The new_dataset_id argument is required' if args.new_dataset_id.nil?
end

namespace :domo do
  desc 'Migrate logstash-output-domo queue from one Domo Dataset to another.'
  task :migrate_queue, [:old_dataset_id, :new_dataset_id, :queue_settings] do |t, args|
    $LOAD_PATH.unshift(File.join(File.dirname(__dir__), "lib"))
    require "redis"
    require "yaml"
    require "domo/client"
    require "domo/queue/redis"

    args.with_defaults(:queue_settings => './testing/rspec_settings.yaml')

    config_file = File.expand_path(args.queue_settings)
    begin
      settings = YAML.safe_load(File.read(config_file))
      validate_settings!(settings, args)

      redis_client = init_redis_client

      domo_settings = settings['domo']
      domo_client = Domo::Client.new(domo_settings['client_id'],
                                     domo_settings['client_secret'],
                                     domo_settings.fetch('api_host', 'api.domo.com'),
                                     true,
                                     Java::ComDomoSdkRequest::Scope::DATA)
    rescue KeyError => e
      puts "#{e} was not found in the settings file #{args.queue_settings}"
      exit(1)
    rescue ArgumentError => e
      puts e
      exit(1)
    end

    begin
      _ = domo_client.dataset(args.old_dataset_id)
    rescue Java::ComDomoSdkRequest::RequestException => e
      puts "Error loading Dataset ID #{args.old_dataset_id}"
      raise e
    end
    begin
      _ = domo_client.dataset(args.new_dataset_id)
    rescue Java::ComDomoSdkRequest::RequestException => e
      puts "Error loading Dataset ID #{args.new_dataset_id}"
      raise e
    end

    begin
      old_stream = domo_client.stream(nil, args.old_dataset_id, false )
      new_stream = domo_client.stream(nil, args.new_dataset_id, false )
    rescue Java::ComDomoSdkRequest::RequestException => e
      puts "Error locating Streams!"
      puts e
      raise e
    end

    old_queue = Domo::Queue::Redis::JobQueue.active_queue(redis_client, args.old_dataset_id, old_stream.getId, 'main')
    new_queue = Domo::Queue::Redis::JobQueue.active_queue(redis_client, args.new_dataset_id, new_stream.getId, 'main')
    num_old_jobs = old_queue.length + old_queue.failures.length
    old_pending_data = old_queue.pending_jobs.length
    until old_queue.processed?(true)
      old_queue.failures.reprocess_jobs!
      job = old_queue.pop

      if job.nil?
        merged_data = Array.new
        while old_queue.pending_jobs.merge_ready?(0, 0, true )
          merged_data = old_queue.pending_jobs.reduce(merged_data, 0, true)
          break if merged_data.length == 0

        end
        new_queue << Domo::Queue::Job.new(merged_data, 0) unless merged_data.length <= 0
      else
        job.data_part = nil
        new_queue << job
      end
    end

    puts "Successfully migrated #{num_old_jobs} jobs from #{args.old_dataset_id} to #{args.new_dataset_id}"
    puts "Successfully migrated #{old_pending_data} rows from the pending queue to #{args.new_dataset_id}"
  end
end
