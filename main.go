package main

import (
	"net"

	"github.com/unchartedsoftware/distil-pipeline-server/pipeline"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

func main() {
	lis, err := net.Listen("tcp", "localhost:9500")
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pipeline.RegisterPipelineComputeServer(grpcServer, pipeline.NewServer())
	grpcServer.Serve(lis)
}
