default: test
test:
	docker-compose build
	docker-compose run --rm test