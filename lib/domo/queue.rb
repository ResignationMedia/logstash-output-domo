# encoding: utf-8
require "securerandom"
require "json"
require "thread"
require "domo/queue/redis"
require "domo/queue/thread"

module Domo
  module Queue
    # Job that goes into a Queue and handles sending event data using the DOMO Streams API.
    class Job
      # @return [String] A unique ID for the job.
      attr_reader :id
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
      def initialize(event, data, part_num=nil, execution_id=nil, id=nil)
        @event = event
        @data = data
        @part_num = part_num
        if @part_num.is_a? java.util.concurrent.atomic.AtomicInteger
          @part_num = @part_num.get
        end
        @execution_id = execution_id

        @id = id.nil? ? SecureRandom.uuid : id
      end

      # Construct the class from a JSON string.
      #
      # @param json_str [String] The JSON string to decode into a constructor.
      # @return [Job]
      def self.from_json!(json_str)
        json_hash = JSON.parse(json_str, {:symbolize_names => true})

        self.new(json_hash[:event], json_hash[:data], json_hash[:part_num],
                 json_hash[:execution_id], json_hash[:id])
      end

      # Convert the class's (important) attributes to a JSON string.
      # Useful for storing the object in redis (shocker)
      #
      # @return [String] The JSON encoded string.
      def to_json
        json_hash = {
            :id           => @id,
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
      # @return [Hash]
      def to_hash
        {
            :id           => @id,
            :event        => @event.to_hash,
            :data         => @data,
            :part_num     => @part_num,
            :execution_id => @execution_id,
        }
      end
    end
  end
end
