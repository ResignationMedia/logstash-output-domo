source 'https://rubygems.org'
gemspec

# The csv codec plugin is still in development and is not available on rubygems.org
csv_codec_source = File.join(File.dirname(File.dirname(__FILE__ )), 'logstash-codec-csv')
if File.directory?(csv_codec_source)
  gem 'logstash-codec-csv', '0.9.0', :path => csv_codec_source
end
