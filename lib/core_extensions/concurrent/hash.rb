require "concurrent"

module CoreExtensions
  refine Concurrent::Hash do
    def execution_id
      begin
        @execution_id
      rescue NameError
        @execution_id = nil
        @execution_id
      end
    end

    def execution_id=(execution_id)
      if execution_id == 0
        @execution_id = nil
      else
        @execution_id = execution_id
      end
    end
  end
end