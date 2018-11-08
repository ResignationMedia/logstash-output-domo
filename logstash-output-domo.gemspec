Gem::Specification.new do |s|
  s.name          = 'logstash-output-domo'
  s.version       = '0.1.1'
  s.licenses      = ['Apache-2.0']
  s.summary       = 'Output events to the DOMO Streams API.'
  #s.description   = 'TODO: Write a longer description or delete this line.'
  #s.homepage      = 'TODO: Put your plugin''s website or public repo URL here.'
  s.authors       = ['Chris Brundage', 'Chive Media Group, LLC']
  s.email         = 'chris.brundage@chivemediagroup.com'
  s.platform      = 'java'
  s.require_paths = ['lib', 'vendor/jar-dependencies']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT', 'vendor/jar-dependencies/**/*.jar', 'vendor/jar-dependencies/**/*.rb']

   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Jar dependencies
  s.requirements << "jar 'com.squareup.okhttp3:okhttp', '3.7.0'"
  s.requirements << "jar 'com.squareup.okhttp3:logging-interceptor', '3.7.0'"
  s.requirements << "jar 'com.google.code.gson:gson', '2.8.0'"
  s.requirements << "jar 'org.jetbrains.kotlin:kotlin-stdlib', '1.3.0'"
  
  s.requirements << "jar 'org.apache.commons:commons-io', '1.3.2'"

  s.requirements << "jar 'org.slf4j:slf4j-api', '1.7.21'"
  s.requirements << "jar 'com.squareup.okio:okio', '2.1.0'"
  #s.requirements << "jar 'com.domo:domo-java-sdk-all', '0.4.0'"

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", "~> 2.0"
  s.add_runtime_dependency "logstash-codec-plain"
  s.add_runtime_dependency "logstash-codec-csv", "=0.1.6"
  s.add_runtime_dependency "jar-dependencies"
  s.add_runtime_dependency "thread_safe", "~> 0.3.6"

  s.add_development_dependency "logstash-devutils"
end