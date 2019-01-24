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
      # @return [LogStash::Event] The job's Logstash Event.
      attr_accessor :event
      # @return [String] A CSV string of the event's data.
      attr_accessor :data
      # @return [Integer] The Stream Execution ID.
      attr_accessor :execution_id

      # @!attribute [r] part_num
      # The Streams API part number.
      # @return [Integer]
      def part_num
        return @part_num.get if @part_num.is_a? java.util.concurrent.atomic.AtomicInteger
        @part_num
      end

      # @!attribute [w]
      def part_num=(part_num)
        if part_num.is_a? java.util.concurrent.atomic.AtomicInteger
          @part_num = part_num.get
        else
          @part_num = part_num
        end
      end

      # @param event [LogStash::Event]
      # @param data [String]
      # @param part_num [Integer, java.util.concurrent.atomic.AtomicInteger, nil]
      # @param execution_id [Integer, nil]
      # @param id [Integer, nil] A unique ID for the job. Do not set this yourself. It will be auto generated, or set from the JSON serialized instance in redis.
      # @param timestamp [String, DateTime] The timestamp when the job was created. Set to now (UTC) if not provided.
      def initialize(event, data, part_num=nil, execution_id=nil, id=nil, timestamp=nil)
        @event = event
        @data = data
        @part_num = part_num
        if @part_num.is_a? java.util.concurrent.atomic.AtomicInteger
          @part_num = @part_num.get
        end
        @execution_id = execution_id

        @id = id.nil? ? SecureRandom.uuid : id
        # Parse the timestamp from a string into a date, if possible.
        unless timestamp.nil?
          case timestamp.class
          when DateTime
            timestamp = timestamp.to_time
          when Date
            timestamp = timestamp.to_time
          when Float
            timestamp = Time.at(timestamp)
          when Integer
            timestamp = Time.at(timestamp)
          when String
            begin
              timestamp = DateTime.parse(timestamp).to_time
            rescue ArgumentError
              timestamp = nil
            end
          else
            timestamp = timestamp
          end

        end
        # Either generate or set the timestamp
        @timestamp = timestamp.nil? ? Time.now.utc : timestamp
      end

      # Construct the class from a JSON string.
      #
      # @param json_str [String] The JSON string to decode into a constructor.
      # @return [Job]
      def self.from_json!(json_str)
        json_hash = JSON.parse(json_str, {:symbolize_names => true})

        self.new(json_hash[:event], json_hash[:data], json_hash[:part_num],
                 json_hash[:execution_id], json_hash[:id], json_hash[:timestamp])
      end

      # Convert the class's (important) attributes to a JSON string.
      # Useful for storing the object in redis (shocker)
      #
      # @return [String] The JSON encoded string.
      def to_json
        json_hash = {
            :id           => @id,
            :timestamp    => @timestamp.to_s,
            :event        => @event,
            :data         => @data,
            :part_num     => @part_num,
            :execution_id => @execution_id,
        }

        JSON.generate(json_hash)
      end

      # Return a hash representation of the Job.
      # Useful for debugging.
      #
      # @param exclude_event [Boolean] Exclude the Event + Data from the Hash output. This is useful for logging.
      # @return [Hash]
      def to_hash(exclude_event=false)
        job = {
            :id           => @id,
            :timestamp    => @timestamp,
            :event        => @event.to_hash,
            :data         => @data,
            :part_num     => @part_num,
            :execution_id => @execution_id,
        }
        if exclude_event
          _ = job.delete(:event)
          _ = job.delete(:data)
        end
        job
      end
    end
  end
end
