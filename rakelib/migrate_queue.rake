def symbolize_redis_client_args(redis_client_args)
  redis_client_args = redis_client_args.inject({}) {|memo, (k, v)| memo[k.to_sym] = v; memo}

  unless redis_client_args.fetch(:sentinels, nil).nil?
    redis_client_args[:sentinels] = redis_client_args[:sentinels].map do |sentinel|
      sentinel.inject({}) {|memo, (k, v)| memo[k.to_sym] = v; memo}
    end
  end

  redis_client_args
end


def validate_settings!(settings)
  raise KeyError, 'domo' unless settings.has_key?('domo')
  raise KeyError, 'redis' unless settings.has_key?('redis')
end


namespace :domo do
  task :queue_migrate, [:old_dataset_id, :new_dataset_id, :queue_settings] do |t, args|
    require "redis"
    require "yaml"
    require "domo/client"
    require "domo/queue/redis"

    args.with_defaults(:queue_settings => './queue_migrate.yaml')

    config_file = File.expand_path(args.queue_settings)
    begin
      settings = YAML.safe_load(File.read(config_file))
      validate_settings!(settings)

      redis_settings = symbolize_redis_client_args(settings['redis'])
      redis_client = Redis.new(**redis_settings)

      domo_settings = settings['domo']
      domo_client = Domo::Client.new(domo_settings['client_id'],
                                     domo_settings['client_secret'],
                                     domo_settings.fetch('api_host', 'api.domo.com'),
                                     true,
                                     Java::ComDomoSdkRequest::Scope::DATA)
    rescue KeyError => e
      puts "#{e} was not found in the settings file #{args.queue_settings}"
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
      old_stream = domo_client.stream(nil, args.old_dataset_id)
      new_stream = domo_client.stream(nil, args.new_dataset_id)
    rescue Java::ComDomoSdkRequest::RequestException => e
      puts "Error locating Streams!"
      puts e
      raise e
    end

    old_queue = Domo::Queue::Redis::JobQueue.active_queue(redis_client, args.old_dataset_id, old_stream.getId, 'main')
    new_queue = Domo::Queue::Redis::JobQueue.active_queue(redis_client, args.new_dataset_id, new_stream.getId, 'main')

    num_old_jobs = old_queue.length + old_queue.failures.length
    old_pending_data = old_queue.pending_jobs.length
    until old_queue.all_empty?
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
