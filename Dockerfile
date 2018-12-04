FROM jruby:9.1.17
RUN mkdir /logstash-output-domo
WORKDIR /logstash-output-domo
COPY . /logstash-output-domo
RUN bundle install