#!/bin/bash

# get veldt-ingest and force a static rebuild of it so that it can run on Alpine
cd ..
make build_static
mv distil-pipeline-server deploy
cd deploy

# builds distil docker image
docker build -t docker.uncharted.software/distil-pipeline-server:0.7.0 -t docker.uncharted.software/distil-pipeline-server:latest ..
