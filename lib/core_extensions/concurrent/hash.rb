require "concurrent"

module CoreExtensions
  # Add attributes and methods to the class that allow it to behave like a Domo::Queue class.
  refine Concurrent::Hash do
    # @return [String]
    attr_accessor :pipeline_id

    # @!attribute [r] execution_id
    # A Domo Stream Execution ID.
    # @return [Integer, nil]
    def execution_id
      begin
        @execution_id
      rescue NameError
        @execution_id = nil
        @execution_id
      end
    end

    # @!attribute [w] execution_id
    # Set the Stream Execution ID.
    # @param execution_id [Integer, nil]
    def execution_id=(execution_id)
      if execution_id == 0
        @execution_id = nil
      else
        @execution_id = execution_id
      end
    end
  end
end