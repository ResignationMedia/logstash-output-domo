# Logstash Plugin

This is a plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

## Documentation

Logstash provides infrastructure to automatically generate documentation for this plugin. We use the asciidoc format to write documentation so any comments in the source code will be first converted into asciidoc and then into html. All plugin documentation are placed under one [central location](http://www.elastic.co/guide/en/logstash/current/).

- For formatting code or config example, you can use the asciidoc `[source,ruby]` directive
- For more asciidoc formatting tips, see the excellent reference here https://github.com/elastic/docs#asciidoc-guide

## Need Help?

Need help? Try #logstash on freenode IRC or the https://discuss.elastic.co/c/logstash discussion forum.

## Developing

### 1. Plugin Developement and Testing

#### Prerequisites
- [A Domo Account](https://www.domo.com/)
- A ClientID and Client Secret associated with [a Domo App that has Data scope](https://developer.domo.com/docs/authentication/overview-4#Creating%20a%20client)
- Docker
- JRuby 9.1.13.0
- Version 1.17.2 of the Bundler gem
- make

#### Code
- Install dependencies and build the Docker images
```sh
make libbuild
make build
```

#### Test
Before testing for the first time copy `testing/rspec_settings.yaml.example` to `testing/rspec_settings.yaml` and populate it with your Client ID and Client Secret.

##### Basic Testing
```sh
make test
```

##### Advanced Testing
###### Rspec tags
The Makefile can dynamically pass arguments to `rspec` in the Docker container based on the following environment variables:
- `RSPEC_TAGS` - A space separated list of tags to pass via the `--tag` `rspec` switch.
- `RSPEC_ARGS` - A space separated list of arbitrary arguments to pass to `rspec`. `--` should be **excluded** from the args.
- `KEEP_FAILED_DATASETS` - If this environment variable exists at all, then Datasets on failing tests will **NOT** be deleted.

Any combination of the above variables can be exported before running `make test`. If not, then all `rspec` tests will be run with default arguments.

If you're really curious/adventurous feel free to review the `Makefile`

### 2. Running your unpublished Plugin in Logstash
#### 2.1 Prerequisites
- You'll need at least one Redis instance to connect to for the queue.
- You'll want to review the various configuration options for the plugin.

#### 2.2 Run in a local Logstash clone

- Edit Logstash `Gemfile` and add the local plugin path, for example:
```ruby
gem "logstash-output-domo", :path => "/your/local/logstash-output-domo"
```
- Install plugin
```sh
bin/logstash-plugin install --no-verify
```
- Run Logstash with your plugin
```sh
bin/logstash -e 'output {domo {}}'
```
At this point any modifications to the plugin code will be applied to this local Logstash setup. After modifying the plugin, simply rerun Logstash.

#### 2.2 Run in an installed Logstash

You can use the same **2.2** method to run your plugin in an installed Logstash by editing its `Gemfile` and pointing the `:path` to your local plugin development directory or you can build the gem and install it using:

- Build your plugin gem
```sh
gem build logstash-output-domo.gemspec
```
- Install the plugin from the Logstash home
```sh
bin/logstash-plugin install /your/local/plugin/logstash-output-domo.gem
```
- Start Logstash and proceed to test the plugin

## Contributing

Pull requests are more than welcome. Just follow the standard pull request procedure (clone, branch, and request). **ALL** unit tests must pass for a pull request to be considered for review.

Do note that this plugin was developed first and foremost for [our company's](https://atmosphere.tv/) internal usage. We chose to make this open source to keep with the spirit of Elastic's open source model, not so we could spend a lot of time providing support to plugin users. As such, we cannot provide any guarantees on how long it will take to approve pull requests or review bugs.

Do **NOT** reach out to our company's support regarding issues with this plugin. Please instead create an Issue on Github. 