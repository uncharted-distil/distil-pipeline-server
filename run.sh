#!/bin/bash
witch --cmd="make compile && make fmt && go run main.go" --watch="main.go,pipeline/**/*.go" --ignore=""
