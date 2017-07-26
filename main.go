package main

import (
	"net"
	"os"
	"strings"

	"github.com/unchartedsoftware/distil-pipeline-server/pipeline"
	log "github.com/unchartedsoftware/plog"
	"google.golang.org/grpc"
)

const (
	defPort = ":9500"
)

func main() {
	port := os.Getenv("PIPELINE_SERVER_PORT")
	if port == "" {
		port = defPort
	}
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}
	log.Infof("Distil pipeline test server listening on %s", port)
	lis, err := net.Listen("tcp", defPort)
	if err != nil {
		log.Errorf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pipeline.RegisterPipelineComputeServer(grpcServer, pipeline.NewServer())
	grpcServer.Serve(lis)
}
