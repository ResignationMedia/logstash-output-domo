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

  def update_test_settings(settings)
    test_settings_file = File.dirname(File.dirname(__FILE__))
    test_settings_file = File.join(test_settings_file, 'testing')
    test_settings_file = File.join(test_settings_file, 'rspec_settings.yaml')

    File.open(test_settings_file, 'w') do |f|
      YAML.dump(settings, f)
    end
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

    def bootstrap_dataset(test_settings, domo_client)
      stream_id = test_settings.fetch("stream_id", nil)
      dataset_id = test_settings.fetch("dataset_id", nil)

      if dataset_id.nil? and stream_id.nil?
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
      elsif stream_id.nil? or dataset_id.nil?
        raise RuntimeError.new "Specify a DatasetID and StreamID or don't specify anything. I'm too lazy right now to make Rspec sort it out."
      end
    end

    bootstrap_dataset(get_test_settings, domo_client)
  end
end