#!/bin/bash

# get veldt-ingest and force a static rebuild of it so that it can run on Alpine
cd ..
make build_static
mv ta2-server deploy
cd deploy

# builds distil docker image
docker build -t registry.datadrivendiscovery.org/uncharted/ta2-server:0.4.1 -t registry.datadrivendiscovery.org/uncharted/ta2-server:latest ..
