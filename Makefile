default : test
.PHONY: docker-test test
docker-test test :
	-docker-compose run --rm test
	docker-compose down
.PHONY: redis-test redlock-test
redis-test redlock-test :
	-docker-compose run --rm test bundle exec rspec --tag redis_queue --tag redlock --tag ~thread_lock --backtrace
	docker-compose down
.PHONY: rspec-test thread-test
rspec-test thread-test :
	bundle exec rspec --backtrace --tag thread_lock --tag ~redis_queue --tag ~redlock
.PHONY: distclean
distclean : clean 
	-rm -rf vendor
.PHONY: clean
clean :
	docker-compose down
.PHONY: build
build :
	bundle update
	docker-compose build
.PHONY: libbuild
libbuild :
	gradle wrapper
	./gradlew vendor
	bundle install
