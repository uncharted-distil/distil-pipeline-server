#!/bin/bash
docker run \
    --name distil-pipeline-server \
    --rm \
    -p 9500:9500 \
    -v ${D3M_DATA_DIR}:${D3M_DATA_DIR} \
    -e PIPELINE_SERVER_RESULT_DIR=${D3M_DATA_DIR} \
    docker.uncharted.software/distil-pipeline-server
