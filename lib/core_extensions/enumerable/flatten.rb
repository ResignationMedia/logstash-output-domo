module CoreExtensions
  module Enumerable
    module Flatten
      # Give credit where it's due https://stackoverflow.com/a/10715242
      # Flattens a nested hash so we can denormalize it for writing to a csv
      # @param parent_prefix [String, nil] The prefix for subkeys of flattened hashes.
      # @return [Hash] The flattened hash.
      def flatten_with_path(parent_prefix = nil)
        res = {}

        self.each_with_index do |elem, i|
          if elem.is_a?(Array)
            k, v = elem
          else
            k, v = i, elem
          end

          key = parent_prefix ? "#{parent_prefix}.#{k}" : k # assign key name for result hash

          if v.is_a? Enumerable
            res.merge!(v.flatten_with_path(key)) # recursive call to flatten child elements
          else
            res[key] = v
          end
        end

        res
      end
    end
  end
end