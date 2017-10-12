package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/unchartedsoftware/distil-pipeline-server/pipeline"
	"github.com/unchartedsoftware/plog"
	"google.golang.org/grpc"
)

const (
	defPort      = ":50051"
	defResultDir = "./results"
	defSendDelay = "5000"
)

var (
	version   = "unset"
	timestamp = "unset"
)

func main() {
	// fetch the result dir
	resultDir := os.Getenv("PIPELINE_SERVER_RESULT_DIR")
	if resultDir == "" {
		resultDir = defResultDir
	}

	// fetch the port to listen on
	port := os.Getenv("PIPELINE_SERVER_PORT")
	if port == "" {
		port = defPort
	}
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	sendDelayEnv := os.Getenv("PIPELINE_SEND_DELAY")
	if sendDelayEnv == "" {
		sendDelayEnv = defSendDelay
	}
	sendDelay, err := strconv.ParseInt(sendDelayEnv, 10, 64)
	if err != nil {
		log.Warnf("Failed to parse PIPELINE_SEND_DELAY val %v as int: %+v", sendDelayEnv, err)
	}

	// generate a user agent string based on version info
	userAgent := fmt.Sprintf("uncharted-test-ta2-%s-%s", version, timestamp)

	log.Infof(userAgent)
	log.Infof("result directory: %s", resultDir)
	log.Infof("listening on %s", port)
	log.Infof("send delay %d", sendDelay)

	lis, err := net.Listen("tcp", defPort)
	if err != nil {
		log.Errorf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pipeline.RegisterCoreServer(grpcServer, pipeline.NewServer(userAgent, resultDir, sendDelay))
	grpcServer.Serve(lis)
}
