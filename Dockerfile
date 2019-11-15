FROM redis AS redis1
COPY --chown=redis:redis testing/redis1.conf /usr/local/etc/redis/redis.conf

FROM redis AS redis2
COPY --chown=redis:redis testing/redis2.conf /usr/local/etc/redis/redis.conf

FROM redis AS sentinel1
COPY --chown=redis:redis testing/redis-sentinel.conf /usr/local/etc/redis/redis-sentinel.conf

FROM redis AS sentinel2
COPY --chown=redis:redis testing/redis-sentinel.conf /usr/local/etc/redis/redis-sentinel.conf

FROM redis AS sentinel3
COPY --chown=redis:redis testing/redis-sentinel.conf /usr/local/etc/redis/redis-sentinel.conf

FROM jruby:9.2.8.0 AS test
RUN mkdir /logstash-output-domo
COPY . /logstash-output-domo
WORKDIR /logstash-output-domo
RUN gem install bundler --version 1.17.3
RUN bundle install