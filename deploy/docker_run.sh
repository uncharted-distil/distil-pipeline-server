#!/bin/bash
docker run \
    --name ta2-server \
    --rm \
    -p 9500:9500 \
    -v ${D3M_DATA_DIR}:${D3M_DATA_DIR} \
    -e PIPELINE_SERVER_RESULT_DIR=${D3M_DATA_DIR} \
    registry.datadrivendiscovery.org/uncharted/ta2-server
