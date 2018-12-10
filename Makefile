default : test
.PHONY: test
test :
	docker-compose build
	docker-compose run --rm test
	docker-compose down
.PHONY: clean
distclean : clean 
	-rm -rf vendor
clean :
	docker-compose down
.PHONY: build
build : libbuild
	bundle install
.PHONY: libbuild
libbuild :
	gradle wrapper
	./gradlew vendor
