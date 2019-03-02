#!/bin/bash

source ./config.sh

# get veldt-ingest and force a static rebuild of it so that it can run on Alpine
cd ..
make build_static
mv distil-pipeline-server deploy
cd deploy

# builds distil docker image
docker build \
    --no-cache \
    --tag $DOCKER_REPO/$DOCKER_IMAGE_NAME:${DOCKER_IMAGE_VERSION} \
    --tag $DOCKER_REPO/$DOCKER_IMAGE_NAME:latest ..
