# encoding: utf-8
require "date"
require "securerandom"
require "json"
require "thread"

module Domo
  module Queue
    # Interface for queue part numbers
    class PartNumber
      # @return [Integer]
      def incr
        raise NotImplementedError.new('#incr must be implemented.')
      end

      # @return [Integer]
      def get
        raise NotImplementedError.new('#get must be implemented.')
      end

      # @param value [Integer]
      def set(value, *args)
        raise NotImplementedError.new('#set must be implemented.')
      end
    end

    # Job that goes into a Queue and handles sending event data using the DOMO Streams API.
    class Job
      # @return [String] A unique ID for the job.
      attr_reader :id
      # @return [DateTime] The job's creation time.
      attr_reader :timestamp
      # @return [Array<String>] CSV strings for all the event data in this job.
      attr_accessor :data
      attr_accessor :data_part
      attr_accessor :minimum_size

      def upload_data
        @data.join("\n")
      end

      def row_count
        @data.length
      end

      def execution_id
        return if @data_part.nil?
        @data_part.execution_id
      end

      def status
        return :complete if @minimum_size <= 0
        return :complete if row_count >= @minimum_size
        :incomplete
      end

      # @param data [Array<String>]
      # @param data_part [Integer, java.util.concurrent.atomic.AtomicInteger, nil]
      # @param id [Integer, nil] A unique ID for the job. Do not set this yourself. It will be auto generated, or set from the JSON serialized instance in redis.
      # @param timestamp [String, DateTime] The timestamp when the job was created. Set to now (UTC) if not provided.
      def initialize(data, minimum_size=0, data_part=nil, id=nil, timestamp=nil)
        @data = data
        @data_part = data_part
        @minimum_size = minimum_size.to_i

        @id = id.nil? ? SecureRandom.uuid : id
        # Parse the timestamp from a string into a date, if possible.
        unless timestamp.nil?
          case timestamp
          when DateTime
            timestamp = timestamp.to_time
          when Date
            timestamp = timestamp.to_time
          when Float
            timestamp = Time.at(timestamp)
          when Fixnum
            timestamp = Time.at(timestamp.to_i)
          when Integer
            timestamp = Time.at(timestamp)
          when String
            begin
              timestamp = DateTime.parse(timestamp).to_time
            rescue ArgumentError
              timestamp = timestamp.to_i
              timestamp = timestamp == 0 ? nil : Time.at(timestamp)
            end
          else
            timestamp = timestamp
          end
        end
        # Either generate or set the timestamp
        @timestamp = timestamp.nil? ? Time.now.utc : timestamp.utc
      end

      # Construct the class from a JSON string.
      #
      # @param json_str [String] The JSON string to decode into a constructor.
      # @return [Job]
      def self.from_json!(json_str)
        json_hash = JSON.parse(json_str, {:symbolize_names => true})
        if json_hash[:data_part].nil?
          data_part = nil
        else
          data_part = RedisPartNumber.from_json!(json_hash[:data_part])
        end

        self.new(json_hash[:data], json_hash[:minimum_size], data_part, json_hash[:id], json_hash[:timestamp])
      end

      # Convert the class's (important) attributes to a JSON string.
      # Useful for storing the object in redis (shocker)
      #
      # @return [String] The JSON encoded string.
      def to_json
        json_hash = {
            :id           => @id,
            :timestamp    => @timestamp.to_i,
            :data         => @data,
            :data_part    => @data_part&.to_json,
            :minimum_size => @minimum_size,
        }

        JSON.generate(json_hash)
      end

      # Return a hash representation of the Job.
      # Useful for debugging.
      #
      # @param exclude_data [Boolean] Exclude the Data from the Hash output. This is useful for logging.
      # @return [Hash]
      def to_hash(exclude_data=false)
        job = {
            :id           => @id,
            :timestamp    => @timestamp,
            :data         => @data,
            :row_count    => row_count,
            :execution_id => execution_id,
            :minimum_size => @minimum_size,
            :status       => status,
            :data_part    => @data_part,
        }
        if exclude_data
          _ = job.delete(:data)
        end
        job
      end
    end
  end
end
