//
//   Copyright © 2019 Uncharted Software Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package main

import (
	"fmt"
	"net"
	"os"

	log "github.com/unchartedsoftware/plog"
	"google.golang.org/grpc"

	"github.com/uncharted-distil/distil-pipeline-server/env"
	"github.com/uncharted-distil/distil-pipeline-server/pipeline"
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
