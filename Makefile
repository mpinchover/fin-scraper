# image name
IMAGE_NAME = markets-scraper

test:
	python -m unittest discover -s test

build:
	docker build -t $(IMAGE_NAME) .

run: build
	docker run -v /dev/shm:/dev/shm -it --env-file .env --rm -p 5001:5001 $(IMAGE_NAME)

clean:
	docker rmi $(IMAGE_NAME)

rebuild: clean build run test

.PHONY: all test clean