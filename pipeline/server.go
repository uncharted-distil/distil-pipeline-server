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
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	protobuf "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/unchartedsoftware/plog"
)

const (
	versionUnset = "version_unset"
)

var apiVersion = versionUnset

// APIVersion is the version of the TA3-TA2 API extracted from the protobuf definition.  Lazily evaluated
// since the protobuf init has to be complete before it can be used.  Doesn't change after initialization.
func APIVersion() string {
	if apiVersion == versionUnset {
		apiVersion = getAPIVersion()
	}
	return apiVersion
}

func getAPIVersion() string {
	// Get the raw file descriptor bytes
	fileDesc := proto.FileDescriptor(E_ProtocolVersion.Filename)
	if fileDesc == nil {
		log.Errorf("failed to find file descriptor for %v", E_ProtocolVersion.Filename)
		return versionUnset
	}

	// Open a gzip reader and decompress
	r, err := gzip.NewReader(bytes.NewReader(fileDesc))
	if err != nil {
		log.Errorf("failed to open gzip reader: %v", err)
		return versionUnset
	}
	defer r.Close()

	b, err := ioutil.ReadAll(r)
	if err != nil {
		log.Errorf("failed to decompress descriptor: %v", err)
		return versionUnset
	}

	// Unmarshall the bytes from the proto format
	fd := &protobuf.FileDescriptorProto{}
	if err := proto.Unmarshal(b, fd); err != nil {
		log.Errorf("malformed FileDescriptorProto: %v", err)
		return versionUnset
	}

	// Fetch the extension from the FileDescriptorOptions message
	ex, err := proto.GetExtension(fd.GetOptions(), E_ProtocolVersion)
	if err != nil {
		log.Errorf("failed to fetch extension: %v", err)
		return versionUnset
	}
	return *ex.(*string)
}

// Server represents a basic distil solution server.
type Server struct {
	userAgent     string
	resultDir     string
	sendDelay     time.Duration
	numUpdates    int
	errPercentage float64
	maxSolutions  int

	sr *ServerRequests
}

// NewServer creates a new solution server instance.  ID maps are initialized with place holder values
// to support tests without explicit calls to session management.
func NewServer(userAgent string, resultDir string, sendDelay int,
	numUpdates int, errPercentage float64, maxSolutions int) *Server {
	server := new(Server)
	server.userAgent = userAgent
	server.resultDir = resultDir
	server.sendDelay = time.Duration(sendDelay) * time.Millisecond
	server.numUpdates = numUpdates
	server.errPercentage = errPercentage
	server.maxSolutions = maxSolutions

	server.sr = NewServerRequests()

	rand.Seed(time.Now().UTC().UnixNano())

	return server
}

func handleError(code codes.Code, err error) error {
	if err != nil {
		log.Errorf("%v", err)
		return status.Error(code, err.Error())
	}
	return nil
}

func handleTypeError(msg interface{}) error {
	return status.Error(codes.Internal, errors.Errorf("unexpected msg type %s", reflect.TypeOf(msg)).Error())
}

// SearchSolutions generates a searchID and returns a SearchResponse immediately
func (s *Server) SearchSolutions(ctx context.Context, req *SearchSolutionsRequest) (*SearchSolutionsResponse, error) {
	log.Infof("Received SearchSolutions - %v", req)

	searchReq, err := s.sr.AddRequest(rootKey, req)
	if err != nil {
		return nil, handleError(codes.Internal, err)
	}

	// NOTE(jtorrez): could get fancy here and kick-off a goroutine that starts generating solution results
	// but leaving that out of first pass dummy results implementation,

	resp := &SearchSolutionsResponse{SearchId: searchReq.GetRequestID()}
	return resp, nil
}

// GetSearchSolutionsResults returns a stream of solution results associated with a previously issued request
func (s *Server) GetSearchSolutionsResults(req *GetSearchSolutionsResultsRequest, stream Core_GetSearchSolutionsResultsServer) error {
	log.Infof("Received GetSearchSolutionsResults - %v", req)

	searchID := req.GetSearchId()
	_, err := s.sr.GetRequest(searchID)
	if err != nil {
		return handleError(codes.InvalidArgument, err)
	}

	// randomly generate number of solutions to "find"
	solutionsFound := rand.Intn(s.maxSolutions) + 1

	wg := sync.WaitGroup{}
	wg.Add(solutionsFound)

	// race condition is intentional - reporting last encountered error is sufficient
	var sendError error

	for i := 0; i < solutionsFound; i++ {

		// stagger solution responses
		randomDelay := rand.Intn(int(s.sendDelay))
		sleepDuration := s.sendDelay + time.Duration(randomDelay)
		time.Sleep(sleepDuration)

		go func() {
			defer wg.Done()

			// Add a request node for the solution itself - it has no associated grpc request
			// object since it is spawned from the search
			solutionReq, err := s.sr.AddRequest(req.GetSearchId(), nil)
			if err != nil {
				sendError = handleError(codes.Internal, err)
				return
			}

			resp := &GetSearchSolutionsResultsResponse{
				SolutionId: solutionReq.GetRequestID(),
				// NOTE(jtorrez): according to comments in proto file, InternalScore field should be NaN
				// if system doesn't have an internal score to provide. i.e., this optional
				// field shouldn't ever be ommited, but it is not possible to set this
				// field to nil in Go, so just generating a random number for now
				InternalScore: rand.Float64(),
				// TODO(jtorrez): omitting the more complicated Scores field (which includes
				// problem specific metrics like F1 score, etc.) until parsing the problem
				// type functionality is added to this stub server
			}
			// wait a random amount of time within a limit before sending found solution
			randomDelay := rand.Intn(int(s.sendDelay))
			sleepDuration := s.sendDelay + time.Duration(randomDelay)
			time.Sleep(sleepDuration)

			// mark the request as a complete
			s.sr.SetComplete(solutionReq.GetRequestID())

			err = stream.Send(resp)
			if err != nil {
				log.Error(err)
				sendError = err
				return
			}
		}()
	}
	wg.Wait()

	return sendError
}

// EndSearchSolutions Releases resources associated with a previusly issued search request.
// NOTE(cbethune): Does this require that a Stop request has been issued?
func (s *Server) EndSearchSolutions(ctx context.Context, req *EndSearchSolutionsRequest) (*EndSearchSolutionsResponse, error) {
	log.Infof("Received EndSearchSolutions - %v", req)
	searchID := req.GetSearchId()

	_, err := s.sr.GetRequest(searchID)
	if err != nil {
		return nil, handleError(codes.InvalidArgument, err)
	}

	err = s.sr.RemoveRequest(searchID)
	if err != nil {
		return nil, handleError(codes.Internal, err)
	}

	return &EndSearchSolutionsResponse{}, nil
}

// StopSearchSolutions Stops a running solution search request.
// NOTE(cbethune): Does this allow for a search to be restarted via a search request that uses the
// same ID?
func (s *Server) StopSearchSolutions(ctx context.Context, req *StopSearchSolutionsRequest) (*StopSearchSolutionsResponse, error) {
	log.Infof("Received StopSearchSolutions - %v", req)
	searchID := req.GetSearchId()
	_, err := s.sr.GetRequest(searchID)
	if err != nil {
		return nil, handleError(codes.InvalidArgument, err)
	}

	// mark the request as complete - score, fit, produce and still execute
	s.sr.SetComplete(searchID)

	return &StopSearchSolutionsResponse{}, nil
}

// DescribeSolution generates a solution description struct for a given solution.
func (s *Server) DescribeSolution(ctx context.Context, req *DescribeSolutionRequest) (*DescribeSolutionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// ScoreSolution generates a score for a given solution.
func (s *Server) ScoreSolution(ctx context.Context, req *ScoreSolutionRequest) (*ScoreSolutionResponse, error) {
	log.Infof("Received ScoreSolution - %v", req)

	solutionID := req.GetSolutionId()
	scoreRequest, err := s.sr.AddRequest(solutionID, req)
	if err != nil {
		return nil, handleError(codes.InvalidArgument, err)
	}

	response := &ScoreSolutionResponse{RequestId: scoreRequest.GetRequestID()}
	return response, err
}

// GetScoreSolutionResults returns a stream of solution score results for a previously issued  scoring request.
func (s *Server) GetScoreSolutionResults(req *GetScoreSolutionResultsRequest, stream Core_GetScoreSolutionResultsServer) error {
	log.Infof("Received GetScoreSolutionResults - %v", req)
	scoreID := req.GetRequestId()

	scoreRequest, err := s.sr.GetRequest(scoreID)
	if err != nil {
		return handleError(codes.InvalidArgument, err)
	}

	scoreMsg, ok := scoreRequest.GetRequestMsg().(*ScoreSolutionRequest)
	if !ok {
		return handleTypeError(scoreRequest.GetRequestMsg())
	}

	// reflect the request scoring metric and give it some random data
	metrics := scoreMsg.GetPerformanceMetrics()
	scores := []*Score{}
	for _, metric := range metrics {
		scores = append(scores, &Score{
			Metric: metric,
			Value: &Value{
				Value: &Value_Raw{
					Raw: &ValueRaw{
						Raw: &ValueRaw_Double{
							Double: rand.Float64(),
						},
					},
				},
			},
		})
	}

	// sleep for a bit
	start := time.Now()
	randomDelay := rand.Intn(int(s.sendDelay))
	sleepDuration := s.sendDelay + time.Duration(randomDelay)
	time.Sleep(sleepDuration)
	end := time.Now()

	// convert times to protobuf timestamp format
	tsStart, err := ptypes.TimestampProto(start)
	if err != nil {
		return handleError(codes.Internal, err)
	}
	tsEnd, err := ptypes.TimestampProto(end)
	if err != nil {
		log.Error(err)
		return handleError(codes.Internal, err)
	}

	// create response structure
	scoreResult := &GetScoreSolutionResultsResponse{
		Progress: &Progress{
			State: ProgressState_COMPLETED,
			Start: tsStart,
			End:   tsEnd,
		},
		Scores: scores,
	}

	// mark the request as complete
	s.sr.SetComplete(scoreID)

	// send response
	err = stream.Send(scoreResult)
	if err != nil {
		return handleError(codes.Internal, err)
	}
	return nil
}

// FitSolution fits a solution to training data.
func (s *Server) FitSolution(ctx context.Context, req *FitSolutionRequest) (*FitSolutionResponse, error) {
	log.Infof("Received FitSolution - %v", req)

	solutionID := req.GetSolutionId()
	fitRequest, err := s.sr.AddRequest(solutionID, req)
	if err != nil {
		return nil, handleError(codes.InvalidArgument, err)
	}

	// save the request ID and mark the fit as incomplete (produce can't execute on an incomplete fit)
	response := &FitSolutionResponse{
		RequestId: fitRequest.GetRequestID(),
	}
	return response, nil
}

// GetFitSolutionResults returns a stream of solution fit result for a previously issued fit request.
func (s *Server) GetFitSolutionResults(req *GetFitSolutionResultsRequest, stream Core_GetFitSolutionResultsServer) error {
	log.Infof("Received GetFitSolutionResults - %v", req)

	fitID := req.GetRequestId()
	_, err := s.sr.GetRequest(fitID)
	if err != nil {
		return handleError(codes.InvalidArgument, err)
	}

	// apply a random delay
	start := time.Now()
	randomDelay := rand.Intn(int(s.sendDelay))
	sleepDuration := s.sendDelay + time.Duration(randomDelay)
	time.Sleep(sleepDuration)
	end := time.Now()

	// mark the fit for the solution as complete
	s.sr.SetComplete(fitID)

	// convert times to protobuf timestamp format
	tsStart, err := ptypes.TimestampProto(start)
	if err != nil {
		return handleError(codes.Internal, err)
	}
	tsEnd, err := ptypes.TimestampProto(end)
	if err != nil {
		return handleError(codes.Internal, err)
	}

	// add a node to hold the fitted request ID
	fittedSolution, err := s.sr.AddRequest(fitID, nil)
	if err != nil {
		return handleError(codes.InvalidArgument, err)
	}

	// create the response message
	fitResult := &GetFitSolutionResultsResponse{
		Progress: &Progress{
			State: ProgressState_COMPLETED,
			Start: tsStart,
			End:   tsEnd,
		},
		FittedSolutionId: fittedSolution.GetRequestID(),
	}

	// send it back to the caller
	err = stream.Send(fitResult)
	if err != nil {
		return handleError(codes.Internal, err)
	}

	return nil
}

// ProduceSolution executes a solution on supplied data.  Solution needs to have previously executed a fit.
func (s *Server) ProduceSolution(ctx context.Context, req *ProduceSolutionRequest) (*ProduceSolutionResponse, error) {
	log.Infof("Received ProduceSolution - %v", req)

	produceRequest, err := s.sr.AddRequest(req.GetFittedSolutionId(), req)
	if err != nil {
		return nil, handleError(codes.InvalidArgument, err)
	}

	response := &ProduceSolutionResponse{RequestId: produceRequest.GetRequestID()}
	return response, nil
}

// GetProduceSolutionResults returns a stream of solution results for a previously issued produce request.
func (s *Server) GetProduceSolutionResults(req *GetProduceSolutionResultsRequest, stream Core_GetProduceSolutionResultsServer) error {
	log.Infof("Received GetProduceSolutionResults - %v", req)
	produceID := req.GetRequestId()

	if rand.Float64() < s.errPercentage {
		return handleError(codes.InvalidArgument, fmt.Errorf("randomly generated error"))
	}

	produceRequest, err := s.sr.GetRequest(produceID)
	if err != nil {
		return handleError(codes.Internal, err)
	}

	// apply a random delay
	start := time.Now()
	randomDelay := rand.Intn(int(s.sendDelay))
	sleepDuration := s.sendDelay + time.Duration(randomDelay)
	time.Sleep(sleepDuration)
	end := time.Now()

	// convert times to protobuf timestamp format
	tsStart, err := ptypes.TimestampProto(start)
	if err != nil {
		handleError(codes.Internal, err)
	}
	tsEnd, err := ptypes.TimestampProto(end)
	if err != nil {
		handleError(codes.Internal, err)
	}

	produceRequestMsg, ok := produceRequest.GetRequestMsg().(*ProduceSolutionRequest)
	if !ok {
		handleTypeError(produceRequest.GetRequestMsg())
	}

	// we only look at first output and expect it to be a dataset URI
	inputs := produceRequestMsg.GetInputs()
	if len(inputs) != 1 {
		return handleError(codes.Internal, errors.Errorf("expecting single input in request, found %d", len(inputs)))
	}

	// pull the dataset URI out of the produce request
	datasetURIValue, ok := inputs[0].GetValue().(*Value_DatasetUri)
	if !ok {
		handleTypeError(inputs[0].GetValue())
	}

	// Get the problem from the search request.  Search request is retrieved
	// by traversing ancestors and testing the saved message type
	parentID := produceRequest.GetParent()
	var searchRequestMsg *SearchSolutionsRequest
	for parentID != "" {
		parentRequest, err := s.sr.GetRequest(parentID)
		if err != nil {
			return handleError(codes.Internal, err)
		}
		searchRequestMsg, ok = parentRequest.GetRequestMsg().(*SearchSolutionsRequest)

		if ok {
			break
		}
		parentID = parentRequest.GetParent()
	}

	// Check if a primitive rather than a complete solution request is being run.
	var resultURI string
	log.Infof("Checking for primitive request")

	log.Infof("Processing solution request")
	problem := searchRequestMsg.GetProblem()

	taskType := problem.GetProblem().GetTaskType()

	problemInputs := problem.GetInputs()
	if len(problemInputs) != 1 {
		return handleError(codes.Internal, errors.Errorf("expecting single input in problem, found %d", len(problemInputs)))
	}

	problemTargets := problemInputs[0].GetTargets()
	if len(problemTargets) != 1 {
		return handleError(codes.Internal, errors.Errorf("expecting single target in problem, found %d", len(problemTargets)))
	}

	targetName := problemTargets[0].GetColumnName()

	// create mock result data
	resultURI, err = createResults(produceRequestMsg.GetFittedSolutionId(), datasetURIValue.DatasetUri, s.resultDir, targetName, taskType)
	if err != nil {
		return handleError(codes.Internal, errors.Errorf("Failed to generate result data for solution `%s`: %v", produceRequestMsg.GetFittedSolutionId(), err))
	}

	exposedOutputs := map[string]*Value{
		"outputs.0": {
			Value: &Value_CsvUri{
				CsvUri: resultURI,
			},
		},
	}

	// create a response emessage
	produceResults := &GetProduceSolutionResultsResponse{
		Progress: &Progress{
			State: ProgressState_COMPLETED,
			Start: tsStart,
			End:   tsEnd,
		},
		ExposedOutputs: exposedOutputs,
	}

	// send it back to the caller
	err = stream.Send(produceResults)
	if err != nil {
		handleError(codes.Internal, err)
	}
	return nil
}

// SolutionExport exports a previously generated solution.  The solution needs to have had a fit step
// executed on it to be valid for export.
func (s *Server) SolutionExport(ctx context.Context, req *SolutionExportRequest) (*SolutionExportResponse, error) {
	log.Infof("Received ExportSolution - %v", req)

	solutionID := req.GetSolutionId()
	_, err := s.sr.GetRequest(solutionID)
	if err != nil {
		handleError(codes.InvalidArgument, err)
	}
	response := &SolutionExportResponse{}
	return response, nil
}

// DataAvailable does something
func (s *Server) DataAvailable(ctx context.Context, in *DataAvailableRequest) (*DataAvailableResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// LoadFittedSolution does something
func (s *Server) LoadFittedSolution(ctx context.Context, in *LoadFittedSolutionRequest) (*LoadFittedSolutionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// LoadSolution does something
func (s *Server) LoadSolution(ctx context.Context, in *LoadSolutionRequest) (*LoadSolutionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// SaveSolution does something
func (s *Server) SaveSolution(ctx context.Context, in *SaveSolutionRequest) (*SaveSolutionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// SaveFittedSolution does something
func (s *Server) SaveFittedSolution(ctx context.Context, in *SaveFittedSolutionRequest) (*SaveFittedSolutionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// UpdateProblem updates the problem defintion associated with a search
func (s *Server) UpdateProblem(ctx context.Context, req *UpdateProblemRequest) (*UpdateProblemResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// ListPrimitives returns a list of TA1 primitives that TA3 is allowed to use in pre-processing solution
// specifications.
func (s *Server) ListPrimitives(ctx context.Context, req *ListPrimitivesRequest) (*ListPrimitivesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// Hello returns information on what the server supports.
func (s *Server) Hello(ctx context.Context, req *HelloRequest) (*HelloResponse, error) {
	response := &HelloResponse{
		AllowedValueTypes: []ValueType{ValueType_DATASET_URI},
	}
	return response, nil
}
