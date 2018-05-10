package main

import (
	"fmt"
	"net"
	"os"

	"github.com/unchartedsoftware/plog"
	"google.golang.org/grpc"

	"github.com/unchartedsoftware/distil-pipeline-server/env"
	"github.com/unchartedsoftware/distil-pipeline-server/pipeline"
)

const (
	defPort      = ":45042"
	defResultDir = "./results"
	defSendDelay = "5000"
)

var (
	version   = "unset"
	timestamp = "unset"
)

func main() {
	log.Infof("version: %s built: %s", version, timestamp)

	// load config from env
	config, err := env.LoadConfig()
	if err != nil {
		log.Errorf("%+v", err)
		os.Exit(1)
	}
	log.Infof("%+v", config)

	// generate a user agent string based on version info
	userAgent := fmt.Sprintf("uncharted-test-ta2-%s-%s", version, timestamp)

	lis, err := net.Listen("tcp", config.Port)
	if err != nil {
		log.Errorf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	solutionServer := pipeline.NewServer(
		userAgent,
		config.ResultDir,
		config.SendDelay,
		config.NumUpdates,
		config.ErrPercent,
		config.MaxSolutions)

	pipeline.RegisterCoreServer(grpcServer, solutionServer)
	grpcServer.Serve(lis)
}
