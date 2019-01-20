default : test
.PHONY: docker-test test
docker-test test :
	-docker-compose run --rm test
	docker-compose down
.PHONY: rspec-test
rspec-test :
	bundle exec rspec --backtrace --tag thread_lock --tag ~redis_queue --tag ~redlock
.PHONY: clean
distclean : clean 
	-rm -rf vendor
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
