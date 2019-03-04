.PHONY: docker-test test redlock-test thread-test distclean clean build libbuild

default : build

ifdef RSPEC_TAGS
TAGS = $(shell tags=""; for t in $(RSPEC_TAGS); do tags="$${tags} --tag $$t"; done; echo "$$tags")
else
TAGS =
endif

ifdef RSPEC_ARGS
ARGS = $(shell args=""; for a in $(RSPEC_ARGS); do args="$${args} --$$a"; done; echo "$$args")
else
ARGS =
endif

test : clean
	-docker-compose run --rm test bundle exec rspec --backtrace --format documentation$(ARGS)$(TAGS)
	docker-compose down

redlock-test : clean
ifeq ($(TAGS),)
	-docker-compose run --rm test bundle exec rspec --backtrace --format documentation$(ARGS) --tag redis_queue --tag redlock --tag ~thread_lock
else
	-docker-compose run --rm test bundle exec rspec --backtrace --format documentation$(ARGS)$(TAGS)
endif
	docker-compose down

thread-test :
ifeq ($(TAGS),)
	bundle exec rspec --backtrace --format documentation$(ARGS) --tag thread_lock
else
	bundle exec rspec --backtrace --format documentation$(ARGS)$(TAGS)
endif

distclean : clean
	-rm -rf vendor

clean :
	docker-compose down

build :
	bundle update
	docker-compose build

libbuild :
	gradle wrapper
	./gradlew vendor
	bundle install
