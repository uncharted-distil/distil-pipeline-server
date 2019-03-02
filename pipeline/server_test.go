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

package pipeline

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/unchartedsoftware/plog"
	"google.golang.org/grpc"
)

const (
	port = ":50051"
)

func stubServer(stop chan bool) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Errorf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	server := NewServer(
		"test_agent",
		"",
		0,
		1,
		0.0,
		1)
	RegisterCoreServer(grpcServer, server)

	// handle stop signal to shut down after test
	go func() {
		stopReq := <-stop
		if stopReq {
			grpcServer.Stop()
		}
	}()
	grpcServer.Serve(lis)
}

func setup() (CoreClient, chan bool) {
	// Run the server
	stop := make(chan bool)
	go stubServer(stop)

	// Connect a client - server shuts down via stop signal so we won't
	// worry client-initiated disconnect
	address := fmt.Sprintf("localhost%s", port)
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Errorf("failed to connect to server: %v", err)
	}
	client := NewCoreClient(conn)

	return client, stop
}

// Example test
func TestSearch(t *testing.T) {
	client, stop := setup()

	t.Run("TestSearch", func(t *testing.T) {
		request := &SearchSolutionsRequest{
			AllowedValueTypes: []ValueType{ValueType_DATASET_URI},
		}
		_, err := client.SearchSolutions(context.Background(), request)
		stop <- true
		assert.NoError(t, err)
	})
}
