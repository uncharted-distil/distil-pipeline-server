#!/bin/bash

source ./config.sh

docker push $DOCKER_REPO/$DOCKER_IMAGE_NAME:${DOCKER_IMAGE_VERSION}
docker push $DOCKER_REPO/$DOCKER_IMAGE_NAME:latest
