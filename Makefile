default: test
test:
	docker-compose build
	docker-compose run --rm test
	docker-compose down
clean:
	docker-compose down