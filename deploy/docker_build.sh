#!/bin/bash

# get veldt-ingest and force a static rebuild of it so that it can run on Alpine
env CGO_ENABLED=0 env GOOS=linux GOARCH=amd64 go build ..

# builds distil docker image
docker build -t docker.uncharted.software/distil-pipeline-server:0.1 ..
