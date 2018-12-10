module Domo
  # Interacts with the DOMO APIs.
  class Client
    # @return [Java::ComDomoSdk::DomoClient]
    attr_reader :client
    # @return [Java::ComDomoSdkStreams::StreamClient]
    attr_reader :stream_client

    # @param client_id [String] The OAuth ClientID.
    # @param client_secret [String] The OAuth Client Secret.
    # @param api_host [String] The host for API connections.
    # @param use_https [Boolean] Use HTTPS for API requests.
    # @param scopes [Array<Java::ComDomoSdkRequest::Scope>, Java::ComDomoSdkRequest::Scope] The OAuth permission scopes.
    # @return [Client]
    def initialize(client_id, client_secret, api_host, use_https, scopes)
      unless scopes.is_a? Array
        scopes = [scopes]
      end
      # Build the API client configuration
      client_config = Java::ComDomoSdkRequest::Config.with
                          .clientId(client_id)
                          .clientSecret(client_secret)
                          .apiHost(api_host)
                          .useHttps(use_https)
                          .scope(*scopes)
                          .build()
      # Instantiate our clients
      @client = Java::ComDomoSdk::DomoClient.create(client_config)
      @stream_client = @client.streamClient
    end

    # Enumerator that lazily paginates through various DOMO SDK methods that make use of it.
    #
    # @param method [Method] The DOMO SDK method to be called
    # @param limit [Integer] The limit of results per page
    # @param offset [Integer] The page offset
    # @param args [Array] An array of arguments to be passed to `method`
    # @return [Object] A result from the API call
    def paginate_list(method, limit, offset=0, args=Array.new)
      args << limit
      Enumerator.new do |y|
        results = method.call(*args, offset)
        until results.size <= 0
          results.each do |result|
            y.yield result
          end
          offset += limit
          results = method.call(*args, offset)
        end
      end
    end

    # Get column names from the provided Stream's Dataset
    #
    # @param stream [Java::ComDomoSdkStreamModel::Stream] A DOMO SDK Stream object
    # @return [Array<String>] The Dataset's column names
    def dataset_schema_columns(stream)
      dataset = @client.dataSetClient.get(stream.getDataset.getId)
      schema = dataset.getSchema

      schema.getColumns.map(&:getName)
    end

    # Get the provided Stream's ACTIVE Stream Execution or create a new one
    #
    # @param stream [Java::ComDomoSdkStreamModel::Stream] A DOMO SDK Stream object
    # @param stream_execution [Java::ComDomoSdkStreamModel::Execution, nil] If provided, check for the latest state on the Stream Execution before creating a new one.
    # @return [Java::ComDomoSdkStreamModel::Execution]
    def stream_execution(stream, stream_execution=nil)
      create_execution = @stream_client.java_method :createExecution, [Java::long]
      if stream_execution.nil?
        limit = 50
        offset = 0
        list_executions = @stream_client.java_method :listExecutions, [Java::long, Java::long, Java::long]

        paginate_list(list_executions, limit, offset, [stream.getId]).each do |execution|
          if execution.currentState == "ACTIVE"
            return execution
          end
        end
      else
        stream_execution = @stream_client.getExecution(stream.getId, stream_execution.getId)
        if stream_execution.currentState == "ACTIVE"
          return stream_execution
        end
      end

      create_execution.call(stream.getId)
    end

    # Get a Stream
    #
    # @param stream_id [Integer, nil] The optional ID of the Stream
    # @param dataset_id [String, nil] The ID of the associated Dataset
    # @param include_execution [Boolean] Returns a new or active Stream Execution along with the Stream
    # @return [Array<Java::ComDomoSdkStreamModel::Stream, (Java::ComDomoSdkStreamModel::Execution, nil)>]
    def stream(stream_id=nil, dataset_id=nil, include_execution=true)
      stream_list = @stream_client.java_method :list, [Java::int, Java::int]
      stream_get = @stream_client.java_method :get, [Java::long]

      unless stream_id.nil?
        return stream_get.call(stream_id)
      end

      limit = 500
      offset = 0
      stream = nil
      paginate_list(stream_list, limit, offset).each do |s|
        if s.dataset.getId == dataset_id
          stream = s
          break
        end
      end

      if stream.nil?
        raise DomoStreamNotFound("No Stream found for Dataset #{dataset_id}", dataset_id, stream_id)
      end

      if include_execution
        stream_execution = stream_execution(stream)
      else
        stream_execution = nil
      end
      return stream, stream_execution
    end
  end

  # Raised if a DOMO Stream cannot be found in the API.
  class DomoStreamNotFound < RuntimeError
    # @param msg [String] The error message.
    # @param dataset_id [String, nil] The DOMO DatasetID.
    # @param stream_id [Integer, nil] The DOMO StreamID.
    # @return [DomoStreamNotFound]
    def initialize(msg, dataset_id=nil, stream_id=nil)
      @dataset_id = dataset_id
      @stream_id = stream_id
      super(msg)
    end
  end
end