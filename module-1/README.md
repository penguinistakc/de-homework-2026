# Data Engineering Zoom Camp Homework Module 1

## Question 1. Understanding Docker images

Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container.

What's the version of `pip` in the image?

- 25.3

### Code for Q1:

Running Docker Container:

```bash
docker run -it --rm --entrypoint /bin/bash
```

Find version of pip:

```bash
pip --version
```

## Question 2. Understanding Docker networking and docker-compose

- postgres:5433

