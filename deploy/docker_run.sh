#!/bin/bash
docker run \
    --name distil-pipeline-server \
    --rm \
    -p 9500:9500 \
    docker.uncharted.software/distil-pipeline-server:0.1
