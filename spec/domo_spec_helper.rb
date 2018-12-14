require "logstash/devutils/rspec/spec_helper"
require "logstash/event"
require "java"
require "logstash-output-domo_jars.rb"
require "yaml"

java_import "java.util.ArrayList"
java_import "com.domo.sdk.DomoClient"
java_import "com.domo.sdk.datasets.model.Column"
java_import "com.domo.sdk.datasets.model.CreateDataSetRequest"
java_import "com.domo.sdk.datasets.model.Schema"
java_import "com.domo.sdk.streams.model.Stream"
java_import "com.domo.sdk.streams.model.StreamRequest"
java_import "com.domo.sdk.streams.model.UpdateMethod"

module DomoHelper
  # Reads test related settings from an rspec_settings.yaml file located in the testing directory at the project root.
  # The main use case is for the client_id and client_secret OAuth params for authenticating with the Domo API.
  #
  # @return [Hash]
  def get_test_settings
    test_settings_file = File.dirname(File.dirname(__FILE__))
    test_settings_file = File.join(test_settings_file, 'testing')
    test_settings_file = File.join(test_settings_file, 'rspec_settings.yaml')

    if File.exists?(test_settings_file)
      YAML.load_file(test_settings_file)
    else
      {}
    end
  end

  # Updates the rspec_settings.yaml file with new test data.
  #
  # @param settings [Hash]
  def update_test_settings(settings)
    test_settings_file = File.dirname(File.dirname(__FILE__))
    test_settings_file = File.join(test_settings_file, 'testing')
    test_settings_file = File.join(test_settings_file, 'rspec_settings.yaml')

    File.open(test_settings_file, 'w') do |f|
      YAML.dump(settings, f)
    end
  end

  # Gets a Domo Stream by DatasetID
  #
  # @param domo_client [DomoClient]
  # @param dataset_id [String]
  # @return [Stream]
  def get_stream(domo_client, dataset_id)
    stream_list = domo_client.streamClient.java_method :list, [Java::int, Java::int]
    limit = 500
    offset = 0
    stream = nil

    # Loop through the Streams List endpoint until we find one matching our DatasetID
    results = stream_list.call(limit, offset)
    until results.size <= 0
      results.each do |result|
        if result.dataset.getId == dataset_id
          stream = result
          break
        end
      end
      offset += limit
      results = stream_list.call(limit, offset)
    end

    # If we couldn't find one, then this Dataset ain't gonna work
    if stream.nil?
      raise Exception.new("No Stream found for DatasetID #{dataset_id}")
    end

    stream
  end

  # Create the test Dataset
  #
  # @param test_settings [Hash] The settings read from rspec_settings.yaml
  # @param domo_client [DomoClient]
  def bootstrap_dataset(test_settings, domo_client)
    puts "No test Dataset or Stream specified. Creating one."

    dsr = CreateDataSetRequest.new
    dsr.setName "logstash-output-domo rspec test"
    dsr.setDescription "Created by the rspec tests for the logstash-output-domo plugin"

    columns = ArrayList.new
    columns.add(Column.new(Java::ComDomoSdkDatasetsModel::ColumnType::LONG, "Count"))
    columns.add(Column.new(Java::ComDomoSdkDatasetsModel::ColumnType::STRING, "Event Name"))
    # columns.add(Column.new(Java::ComDomoSdkDatasetsModel::ColumnType::DATETIME, "Event Timestamp"))
    dsr.setSchema(Schema.new(columns))

    stream_request = StreamRequest.new
    stream_request.setDataSet(dsr)
    stream_request.setUpdateMethod(UpdateMethod::APPEND)
    stream = domo_client.streamClient.create(stream_request)

    stream_id = stream.getId
    dataset_id = stream.getDataset.getId

    puts "Created StreamID #{stream_id} for DatasetID #{dataset_id}"
    test_settings["stream_id"] = stream_id
    test_settings["dataset_id"] = dataset_id
    update_test_settings(test_settings)
  end
end

RSpec.configure do |config|
  include DomoHelper

  config.before(:suite) do
    config.include DomoHelper
    test_settings = get_test_settings

    client_config = Java::ComDomoSdkRequest::Config.with
                        .clientId(test_settings["client_id"])
                        .clientSecret(test_settings["client_secret"])
                        .apiHost("api.domo.com")
                        .useHttps(true)
                        .scope(Java::ComDomoSdkRequest::Scope::DATA)
                        .build()
    domo_client = DomoClient.create(client_config)

    stream_id = test_settings.fetch("stream_id", nil)
    dataset_id = test_settings.fetch("dataset_id", nil)

    if dataset_id.nil? and stream_id.nil?
      bootstrap_dataset(test_settings, domo_client)
    elsif stream_id.nil?
      begin
        stream = get_stream(domo_client, dataset_id)
        test_settings["stream_id"] = stream.getId
        puts "Found StreamID #{stream.getId} for DatasetID #{dataset_id}"
        update_test_settings(test_settings)
      rescue => e
        puts e.message
        bootstrap_dataset(test_settings, domo_client)
      end
    else
      stream = domo_client.streamClient.get(stream_id)
      dataset_id = stream.getDataset.getId
      test_settings["dataset_id"] = dataset_id
      puts "Found DatasetID #{dataset_id} associated with StreamID #{stream_id}"
      update_test_settings(test_settings)
    end
  end
end