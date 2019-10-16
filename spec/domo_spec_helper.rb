require "csv"
require "logstash/devutils/rspec/spec_helper"
require "logstash/event"
require "core_extensions/flatten"
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

Hash.include CoreExtensions::Flatten

module DomoHelper
  # Reads test related settings from an rspec_settings.yaml file located in the testing directory at the project root.
  # The main use case is for the client_id and client_secret OAuth params for authenticating with the Domo API.
  #
  # @return [Hash]
  def get_test_settings
    settings = Hash.new
    base_dir = File.dirname(File.dirname(__FILE__ ))
    test_settings_file = File.join(base_dir, 'testing')
    test_settings_file = File.join(test_settings_file, 'rspec_settings.yaml')

    if File.exists?(test_settings_file)
      domo_settings = YAML.load_file(test_settings_file)
      settings.merge!(domo_settings['domo'])
    end

    settings
  end

  # Wait until the subject's commit thread is done executing (if it exists).
  #
  # @param subject [LogStash::Outputs::Domo]
  def wait_for_commit(subject, expect_thread=false, commit_timeout=5)
    queue = subject.get_queue
    unless expect_thread
      return if queue.commit_status != :running and queue.processed?
    end
    until queue.commit_unscheduled? and queue.processed?(true) or (queue.commit_status == :success and queue.processed?)
      sleep(0.1)
    end
  end

  def wait_for_shutdown(subject)
    queue = subject.get_queue
    wait_for_commit(subject, true)
    until queue.all_empty?
      sleep(0.1)
    end
  end

  # Initializes a DomoClient object from our test settings
  #
  # @param test_settings [Hash] The settings read from rspec_settings.yaml
  # @return [DomoClient]
  def get_domo_client(test_settings)
    client_config = Java::ComDomoSdkRequest::Config.with
                        .clientId(test_settings["client_id"])
                        .clientSecret(test_settings["client_secret"])
                        .apiHost("api.domo.com")
                        .useHttps(true)
                        .scope(Java::ComDomoSdkRequest::Scope::DATA)
                        .build()
    DomoClient.create(client_config)
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
  # @param domo_client [DomoClient]
  # @return [Hash]
  def bootstrap_dataset(domo_client, upload_timestamp=nil, partition_field=nil)
    dsr = CreateDataSetRequest.new
    dsr.setName "logstash-output-domo rspec test"
    dsr.setDescription "Created by the rspec tests for the logstash-output-domo plugin"

    dsr.setSchema(Schema.new(test_dataset_columns(upload_timestamp, partition_field)))

    stream_request = StreamRequest.new
    stream_request.setDataSet(dsr)
    stream_request.setUpdateMethod(UpdateMethod::APPEND)
    stream = domo_client.streamClient.create(stream_request)

    stream_id = stream.getId
    dataset_id = stream.getDataset.getId

    attempt = 0
    max_attempts = 5
    dataset_valid = false
    until dataset_valid
      raise Exception.new("There was a problem with creating Dataset ID #{dataset_id}") if attempt >= max_attempts
      dataset_valid = check_dataset(domo_client, dataset_id)
      attempt += 1
    end

    {
        "dataset_id" => dataset_id,
        "stream_id"  => stream_id,
    }
  end

  def check_dataset(domo_client, dataset_id)
    dataset = domo_client.dataSetClient.get(dataset_id)
    !!dataset
  end

  def test_dataset_columns(upload_timestamp=nil, partition_field=nil)
    columns = ArrayList.new
    columns.add(Column.new(Java::ComDomoSdkDatasetsModel::ColumnType::LONG, "Count"))
    columns.add(Column.new(Java::ComDomoSdkDatasetsModel::ColumnType::STRING, "Event Name"))
    columns.add(Column.new(Java::ComDomoSdkDatasetsModel::ColumnType::DATETIME, "Event Timestamp"))
    columns.add(Column.new(Java::ComDomoSdkDatasetsModel::ColumnType::DATE, "Event Date"))
    columns.add(Column.new(Java::ComDomoSdkDatasetsModel::ColumnType::DOUBLE, "Percent"))
    if upload_timestamp
      columns.add(Column.new(Java::ComDomoSdkDatasetsModel::ColumnType::DATETIME, upload_timestamp))
    end
    if partition_field
      columns.add(Column.new(Java::ComDomoSdkDatasetsModel::ColumnType::DATE, partition_field))
    end
    columns
  end

  # Throw out Event data keys that are not being passed to Domo.
  # The values we keep also need to be coerced to Strings since the Dataset Export API sends back CSV data.
  #
  # @param event [LogStash::Event]
  # @return [Hash]
  def event_to_domo_hash(event)
    new_event = {}
    event.to_hash.each do |k, v|
      if v.is_a? LogStash::Timestamp
        v = DateTime.parse(v.to_s)
        v = v.strftime("%Y-%m-%d %H:%M:%S")
      end
      unless k == "@version" or k == "@timestamp"
        if v.nil?
          new_event[k] = nil
        else
          new_event[k] = v.to_s
        end
      end
    end
    new_event
  end

  # Map the name of a header in CSV data to a Domo Dataset's columns.
  #
  # @param columns [ArrayList[Column]] The Dataset's columns
  # @param header [String] The name of the CSV header.
  # @return [Column] The matching Domo Dataset Column.
  def csv_header_to_column(columns, header)
    col = columns.select do |col|
      col.getName == header
    end
    return if col.nil? or col.length <= 0

    col[0]
  end

  # Convert CSV strings embedded in a QueueJob into a Hash
  #
  # @param job [Domo::Queue::Job] The job.
  # @param upload_timestamp [String, nil] The name of the upload_timestamp column, if applicable.
  # @param partition_field [String, nil] The name of the partition_field column, if applicable.
  # @return [Hash] A hash representation of the job's data.
  def job_data_to_hash(job, upload_timestamp=nil, partition_field=nil)
    columns = test_dataset_columns(upload_timestamp, partition_field)
    col_names = columns.map(&:getName)

    data = job.data.map do |d|
      CSV.parse(d)
    end
    data = Hash[col_names.zip(data.flatten)]

    data.reduce({}) do |memo, (k,v)|
      column = csv_header_to_column(columns, k)
      if k == upload_timestamp
        v = DateTime.parse(v)
        v = v.strftime("%Y-%m-%d")
      elsif column.getType == Java::ComDomoSdkDatasetsModel::ColumnType::DATETIME
        v = DateTime.parse(v)
        v = v.strftime("%Y-%m-%d %H:%M:%S")
      end

      memo.merge({k => v})
    end
  end

  # Convert a Logstash event to a CSV string while honoring the Domo schema
  #
  # @param event [LogStash::Event] The Logstash event.
  # @return [String]
  def event_to_csv(event)
    # Convert the event to a hash that only has fields from the Domo schema
    event = event_to_domo_hash(event)
    # Read the column names into an Array
    column_names = test_dataset_columns.map { |c| c.name }

    encode_options = {
        :headers => column_names,
        :write_headers => false,
        :return_headers => false,
    }
    # Create the CSV string
    csv_data = CSV.generate(String.new, encode_options) do |csv_obj|
      data = event.flatten_with_path
      data = data.sort_by { |k, _| column_names.index(k) }.to_h
      csv_obj << data.values
    end
    csv_data.strip
  end

  # Export a Domo Dataset's data to a CSV parsed Hash.
  #
  # @param domo_client [DomoClient] A Domo API client.
  # @param dataset_id [String] The Dataset ID.
  # @return [Hash, nil]
  def export_dataset(domo_client, dataset_id)
    # Sometimes there's lag on the Domo API so we'll retry a couple of times instead of failing tests for no reason.
    attempts = 0
    data = nil
    data_stream = nil
    loop do
      begin
        # @type [IO]
        data_stream = domo_client.dataSetClient.exportData(dataset_id, true).to_io
        data = data_stream.read
        data_stream.close

        return CSV.parse(data, {:headers => true}).map(&:to_h)
      rescue Java::ComDomoSdkRequest::RequestException => e
        raw_code = Domo::Client.request_error_status_code(e)
        status_code = e.getStatusCode
        if status_code < 400 or status_code == 404 or status_code == 406 or status_code >= 500
          if attempts > 3
            puts "Ran out of retries on API errors for DatasetID #{dataset_id}. Status code is #{status_code}. Reason is #{raw_code}"
            raise e
          end
          sleep(2.0*attempts)
        else
          raise e
        end
      ensure
        attempts += 1
        data_stream.close unless data_stream.nil? or data_stream.closed?
      end
    end

    data
  end

  # Check that the fields in the Dataset match the fields in our expected data
  #
  # @param domo_client [DomoClient]
  # @param dataset_id [String]
  # @param expected_data [Array<Hash>, Hash]
  # @return [Boolean]
  def dataset_field_match?(domo_client, dataset_id, expected_data)
    data = export_dataset(domo_client, dataset_id)
    return false unless data

    if expected_data.is_a? Array
      expected_data_row = expected_data[0]
    else
      expected_data_row = expected_data
    end
    if data.is_a? Array
      data_row = data[0]
    else
      data_row = data
    end

    unmatched_fields = Array.new
    expected_data_row.each do |name, v|
      unless data_row.include? name
        unmatched_fields << name
      end
    end

    unmatched_fields.length <= 0
  end

  # Compare expected data to what's actually in the provided Domo Dataset.
  #
  # @param domo_client [DomoClient]
  # @param dataset_id [String]
  # @param expected_data [Array<Hash>, Hash]
  # @param should_fail [Boolean] Controls whether or not error output is displayed
  #   so we don't get spammed on tests that *should* fail.
  # @return [Boolean]
  def dataset_data_match?(domo_client, dataset_id, expected_data, should_fail=false)
    data = export_dataset(domo_client, dataset_id)

    if data.nil?
      unless expected_data.nil?
        puts "Got no data back from Domo."
        puts "Expected data: #{expected_data}"
        return false
      end
      return true
    end

    if expected_data.is_a? Hash
      return false unless data.size == 1
      data = data[0]
    end

    # Sort the expected and actual data so we don't go chasing down row order differences.
    unless data.is_a? Hash
      data.sort! { |a,b| b["Event Name"] <=> a["Event Name"] }
    end
    unless expected_data.is_a? Hash
      expected_data.sort! { |a,b| b["Event Name"] <=> a["Event Name"] }
    end

    unless data == expected_data
      missing_data = Array.new
      expected_data.each do |d|
        unless data.include? d
          missing_data << d
        end
      end
      unless should_fail
        puts "-----"
        puts "Actual data length: #{data.length}"
        puts "Expected data length: #{expected_data.length}"
        puts "-----"
        puts "Missing Data"
        puts missing_data
        puts "-----"
        puts "Actual Data"
        puts data
        puts "-----"
      end
      return false
    end
    true
  end
end

RSpec.configure do |config|
  config.include DomoHelper

  config.around(:each) do |example|
    timeout = example.metadata.has_key?(:slow) ? 90 : 45
    Timeout::timeout(timeout) {
      example.run
    }
  end
end