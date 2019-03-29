require "active_record"
require "date"

module Domo
  module Models
    class Stream < ActiveRecord::Base
      has_many :stream_executions

      def active_execution
        active_executions = :stream_executions.select { |se| se.active }
        if active_executions.nil? or active_executions.length <= 0
          return
        end
        active_executions[0]
      end

      def commit_ready?(commit_delay=nil)
        return true if commit_delay.nil? or commit_delay == 0 or last_commit.nil?
        return true if last_commit + commit_delay <= Time.now.utc
        false
      end

      def commit(timestamp=nil)
        timestamp = Time.now.utc if timestamp.nil?
        update_attribute(:last_commit, timestamp)
        StreamExecution.processing(:id).each do |se|
          se.update_attribute(:active, false)
        end
      end
    end

    class StreamExecution < ActiveRecord::Base
      has_many :data_parts
      after_save :invalidate_data_parts, unless: :active

      scope :processing, ->(stream_id) { where("active = 1 AND stream_id = ?", stream_id) }

      private
      def invalidate_data_parts
        :data_parts.each do |dp|
          dp.update_attribute(:active, false)
        end
      end
    end

    class DataPart < ActiveRecord::Base
    end
  end
end