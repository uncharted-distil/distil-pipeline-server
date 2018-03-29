package pipeline

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"math/rand"
	"reflect"
	// "path/filepath"
	// "strconv"
	// "strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	// uuid generation
	"github.com/satori/go.uuid"
	// data structures
	"github.com/fatih/set"
	// grpc and protobuf
	"github.com/golang/protobuf/proto"
	protobuf "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/golang/protobuf/ptypes"
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

// Server represents a basic distil pipeline server.
type Server struct {
	sessionIDs         *set.Set
	endSessionIDs      *set.Set
	searchIDs          *set.Set
	searchRequests     map[string]*SearchPipelinesRequest
	endSearchIDs       *set.Set
	pipelineIDs        *set.Set
	pipelineProblemIDs map[string]string
	scoreIDs           *set.Set
	scoreRequests      map[string]*ScorePipelineRequest
	fitIDs             *set.Set
	fitCompleteIDs     *set.Set
	fitPipelineIDs     map[string]string
	produceIDs         *set.Set
	produceRequests    map[string]*ProducePipelineRequest
	problemIDs         *set.Set
	problemRequests    map[string]*StartProblemRequest
	userAgent          string
	resultDir          string
	sendDelay          time.Duration
	numUpdates         int
	errPercentage      float64
	maxPipelines       int
}

// NewServer creates a new pipeline server instance.  ID maps are initialized with place holder values
// to support tests without explicit calls to session management.
func NewServer(userAgent string, resultDir string, sendDelay int,
	numUpdates int, errPercentage float64, maxPipelines int) *Server {
	server := new(Server)
	server.sessionIDs = set.New("test-session-id")
	server.endSessionIDs = set.New("test-end-session-id")
	server.searchIDs = set.New()
	server.endSearchIDs = set.New()
	server.searchRequests = make(map[string]*SearchPipelinesRequest)
	server.pipelineIDs = set.New("test-pipeline-id")
	server.pipelineProblemIDs = make(map[string]string)
	server.scoreIDs = set.New()
	server.scoreRequests = make(map[string]*ScorePipelineRequest)
	server.fitIDs = set.New()
	server.fitCompleteIDs = set.New()
	server.fitPipelineIDs = make(map[string]string)
	server.produceIDs = set.New()
	server.produceRequests = make(map[string]*ProducePipelineRequest)
	server.problemIDs = set.New()
	server.problemRequests = make(map[string]*StartProblemRequest)
	server.userAgent = userAgent
	server.resultDir = resultDir
	server.sendDelay = time.Duration(sendDelay) * time.Millisecond
	server.numUpdates = numUpdates
	server.errPercentage = errPercentage
	server.maxPipelines = maxPipelines
	return server
}

// SearchPipelines generates a searchID and returns a SearchResponse immediately
func (s *Server) SearchPipelines(ctx context.Context, req *SearchPipelinesRequest) (*SearchPipelinesResponse, error) {
	log.Infof("Received SearchPipelines - %v", req)

	// generate search_id
	id := uuid.NewV1().String()
	s.searchIDs.Add(id)
	s.searchRequests[id] = req

	// NOTE(jtorrez): could get fancy here and kick-off a goroutine that starts generating pipeline results
	// but leaving that out of first pass dummy results implementation, should also be analyzing request to
	// for problem, problem_id (if that is kept in the API), template, etc.

	resp := &SearchPipelinesResponse{SearchId: id}
	return resp, nil
}

// GetSearchPipelinesResults returns a stream of pipeline results associated with a previously issued request
func (s *Server) GetSearchPipelinesResults(req *GetSearchPipelinesResultsRequest, stream Core_GetSearchPipelinesResultsServer) error {
	log.Infof("Received GetSearchPipelinesResults - %v", req)
	searchID := req.GetSearchId()
	err := s.validateSearch(searchID)
	if err != nil {
		return err
	}

	// retrieve the search request and find the associated problem ID
	searchRequest, ok := s.searchRequests[searchID]
	if !ok {
		log.Errorf("failed to find persisted search request %s", searchID)
		return status.Errorf(codes.Internal, "failed to find persisted search request %s", searchID)
	}
	problemID := searchRequest.GetProblemId()

	// randomly generate number of pipelines to "find"
	pipelinesFound := rand.Intn(s.maxPipelines)

	// it is possible to find no pipelines, but for the sake of testing
	// generate number of "found" pipelines until it is greater than 0
	if pipelinesFound == 0 {
		for pipelinesFound <= 0 {
			pipelinesFound = rand.Intn(s.maxPipelines)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(pipelinesFound)

	// race condition is intentional - reporting last encountered error is sufficient
	var sendError error

	for i := 0; i < pipelinesFound; i++ {
		go func() {
			defer wg.Done()

			pipelineID := uuid.NewV4().String()
			// save the pipeline ID  and its associated problem ID for subsequent calls
			s.pipelineIDs.Add(pipelineID)
			s.pipelineProblemIDs[pipelineID] = problemID

			resp := &GetSearchPipelinesResultsResponse{
				PipelineId: pipelineID,
				// NOTE(jtorrez): according to comments in proto file, InternalScore field should be NaN
				// if system doesn't have an internal score to provide. i.e., this optional
				// field shouldn't ever be ommited, but it is not possible to set this
				// field to nil in Go, so just generating a random number for now
				InternalScore: rand.Float64(),
				// TODO(jtorrez): omitting the more complicated Scores field (which includes
				// problem specific metrics like F1 score, etc.) until parsing the problem
				// type functionality is added to this stub server
			}
			// wait a random amount of time within a limit before sending found pipeline
			randomDelay := rand.Intn(int(s.sendDelay))
			time.Sleep(time.Duration(randomDelay) * time.Millisecond)
			err := stream.Send(resp)
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

func (s *Server) validateSearch(searchID string) error {
	// If the search ID doesn't exist return an error
	// the API doesn't include a way to commnicate that an ID doesn't exist so using
	// the gRPC built in error codes
	if !s.searchIDs.Has(searchID) {
		return status.Errorf(codes.NotFound, "SearchID: %s doesn't exist", searchID)
	}
	if s.endSearchIDs.Has(searchID) {
		// NOTE(jtorrez): not sure if this is appropriate error code, available gRPC codes
		// don't explicilty communicate a ended session/search/other state
		return status.Errorf(codes.ResourceExhausted, "SearchID: %s already ended, resources no longer available", searchID)
	}
	return nil
}

// EndSearchPipelines Releases resources associated with a previusly issued search request.
// NOTE(cbethune): Does this require that a Stop request has been issued?  Do we consider pipelines produced by this
// request as no longer valid for score, fit, and produce calls?
func (s *Server) EndSearchPipelines(ctx context.Context, req *EndSearchPipelinesRequest) (*EndSearchPipelinesResponse, error) {
	log.Infof("Received EndSearchPipelines - %v", req)
	searchID := req.GetSearchId()
	err := s.validateSearch(searchID)
	if err != nil {
		return nil, err
	}

	s.endSearchIDs.Add(searchID)
	s.searchIDs.Remove(searchID)
	return &EndSearchPipelinesResponse{}, nil
}

// StopSearchPipelines Stops a running pipeline search request.
// NOTE(cbethune): Does this allow for a search to be restarted via a search request that uses the
// same ID?
func (s *Server) StopSearchPipelines(ctx context.Context, req *StopSearchPipelinesRequest) (*StopSearchPipelinesResponse, error) {
	log.Infof("Received StopSearchPipelines - %v", req)
	searchID := req.GetSearchId()
	err := s.validateSearch(searchID)
	if err != nil {
		return nil, err
	}
	return &StopSearchPipelinesResponse{}, nil
}

// DescribePipeline generates a pipeline description struct for a given pipeline.
func (s *Server) DescribePipeline(ctx context.Context, req *DescribePipelineRequest) (*DescribePipelineResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

func (s *Server) validatePipeline(pipelineID string) error {
	// If the pipelineID doesn't exist return an error
	// the API doesn't include a way to commnicate that an ID doesn't exist so using
	// the gRPC built in error codes
	if !s.pipelineIDs.Has(pipelineID) {
		return status.Errorf(codes.NotFound, "PipelineID: %s doesn't exist", pipelineID)
	}
	return nil
}

// ScorePipeline generates a score fo a given pipeline.
func (s *Server) ScorePipeline(ctx context.Context, req *ScorePipelineRequest) (*ScorePipelineResponse, error) {
	log.Infof("Received ScorePipeline - %v", req)
	pipelineID := req.GetPipelineId()
	err := s.validatePipeline(pipelineID)
	if err != nil {
		return nil, err
	}
	// save the request
	scoreID := uuid.NewV4().String()
	s.scoreIDs.Add(scoreID)
	s.scoreRequests[scoreID] = req
	response := &ScorePipelineResponse{scoreID}
	return response, err
}

// GetScorePipelineResults returns a stream of pipeline score results for a previously issued  scoring request.
func (s *Server) GetScorePipelineResults(req *GetScorePipelineResultsRequest, stream Core_GetScorePipelineResultsServer) error {
	log.Infof("Received GetScorePipelineResults - %v", req)
	scoreID := req.GetRequestId()

	// make sure the score request is available
	if !s.scoreIDs.Has(scoreID) {
		return status.Errorf(codes.NotFound, "ScoreID: %s doesn't exist", scoreID)
	}
	request, ok := s.scoreRequests[scoreID]
	if !ok {
		log.Errorf("Score request for %s not persisted", scoreID)
		return status.Errorf(codes.Internal, "Score request for %s not persisted", scoreID)
	}

	// reflect the request scoring metric and give it some random data
	metrics := request.GetPerformanceMetrics()
	scores := []*Score{}
	for _, metric := range metrics {
		scores = append(scores, &Score{
			Metric: metric,
			Value: &Value{
				Value: &Value_Double{rand.Float64()},
			},
		})
	}

	// sleep for a bit
	randomDelay := rand.Intn(int(s.sendDelay))
	start := time.Now()
	time.Sleep(time.Duration(randomDelay) * time.Millisecond)
	end := time.Now()

	// convert times to protobuf timestamp format
	tsStart, err := ptypes.TimestampProto(start)
	if err != nil {
		log.Error(err)
		return status.Errorf(codes.Internal, "convert start timestamp error: %s", err)
	}
	tsEnd, err := ptypes.TimestampProto(end)
	if err != nil {
		log.Error(err)
		return status.Errorf(codes.Internal, "convert failed to convert end timestamp error: %s", err)
	}

	// create response structure
	scoreResult := &GetScorePipelineResultsResponse{
		Progress: Progress_COMPLETED,
		Start:    tsStart,
		End:      tsEnd,
		Scores:   scores,
	}

	// send response
	err = stream.Send(scoreResult)
	if err != nil {
		log.Error(err)
		return status.Errorf(codes.Internal, "failed to send score result: %s", err)
	}

	return nil
}

// FitPipeline fits a pipeline to training data.
func (s *Server) FitPipeline(ctx context.Context, req *FitPipelineRequest) (*FitPipelineResponse, error) {
	log.Infof("Received FitPipeline - %v", req)
	pipelineID := req.GetPipelineId()
	err := s.validatePipeline(pipelineID)
	if err != nil {
		return nil, err
	}
	// save the request ID and mark the fit as incomplete (produce can't execute on an incomplete fit)
	fitID := uuid.NewV4().String()
	s.fitIDs.Add(fitID)
	s.fitPipelineIDs[fitID] = pipelineID
	response := &FitPipelineResponse{fitID}
	return response, nil
}

// GetFitPipelineResults returns a stream of pipeline fit result for a previously issued fit request.
func (s *Server) GetFitPipelineResults(req *GetFitPipelineResultsRequest, stream Core_GetFitPipelineResultsServer) error {
	log.Infof("Received GetFitPipelineResults - %v", req)
	fitID := req.GetRequestId()

	// make sure the score request is available
	if !s.fitIDs.Has(fitID) {
		return status.Errorf(codes.NotFound, "FitID: %s doesn't exist", fitID)
	}

	// apply a random delay
	randomDelay := rand.Intn(int(s.sendDelay))
	start := time.Now()
	time.Sleep(time.Duration(randomDelay) * time.Millisecond)
	end := time.Now()

	// mark the fit for the pipeline as complete
	pipelineID, ok := s.fitPipelineIDs[fitID]
	if !ok {
		log.Errorf("Failed to find pipelineID for fitID: %s", fitID)
		return status.Errorf(codes.Internal, "no pipeline ID for fitID: %s", fitID)
	}
	s.fitCompleteIDs.Add(pipelineID)

	// convert times to protobuf timestamp format
	tsStart, err := ptypes.TimestampProto(start)
	if err != nil {
		log.Error(err)
		return status.Errorf(codes.Internal, "convert start timestamp error: %s", err)
	}
	tsEnd, err := ptypes.TimestampProto(end)
	if err != nil {
		log.Error(err)
		return status.Errorf(codes.Internal, "convert failed to convert end timestamp error: %s", err)
	}

	// create the response message
	fitResult := &GetFitPipelineResultsResponse{
		Progress: Progress_COMPLETED,
		Start:    tsStart,
		End:      tsEnd,
	}

	// send it back to the caller
	err = stream.Send(fitResult)
	if err != nil {
		log.Error(err)
		return status.Errorf(codes.Internal, "failed to send score result: %s", err)
	}

	return nil
}

// ProducePipeline executes a pipeline on supplied data.  Pipeline needs to have previously executed a fit.
func (s *Server) ProducePipeline(ctx context.Context, req *ProducePipelineRequest) (*ProducePipelineResponse, error) {
	log.Infof("Received ProducePipeline - %v", req)

	// check to see if the pipeline exists
	pipelineID := req.GetPipelineId()
	err := s.validatePipeline(pipelineID)
	if err != nil {
		return nil, err
	}

	// only allow produce on a pipeline that has had a fit run against it
	if !s.fitCompleteIDs.Has(pipelineID) {
		return nil, status.Errorf(codes.FailedPrecondition, "fit not executed for pipeline %s", pipelineID)
	}

	produceID := uuid.NewV4().String()
	s.produceIDs.Add(produceID)
	s.produceRequests[produceID] = req
	response := &ProducePipelineResponse{produceID}
	return response, nil
}

// GetProducePipelineResults returns a stream of pipeline results for a previously issued produce request.
func (s *Server) GetProducePipelineResults(req *GetProducePipelineResultsRequest, stream Core_GetProducePipelineResultsServer) error {
	log.Infof("Received GetFitPipelineResults - %v", req)
	produceID := req.GetRequestId()

	// make sure the score request is available
	if !s.produceIDs.Has(produceID) {
		return status.Errorf(codes.NotFound, "ProduceID: %s doesn't exist", produceID)
	}

	produceRequest, ok := s.produceRequests[produceID]
	if !ok {
		log.Errorf("Produce request for %s not persisted", produceID)
		return status.Errorf(codes.Internal, "Produce request for %s not persisted", produceID)
	}

	// apply a random delay
	randomDelay := rand.Intn(int(s.sendDelay))
	start := time.Now()
	time.Sleep(time.Duration(randomDelay) * time.Millisecond)
	end := time.Now()

	// convert times to protobuf timestamp format
	tsStart, err := ptypes.TimestampProto(start)
	if err != nil {
		log.Error(err)
		return status.Errorf(codes.Internal, "convert start timestamp error: %s", err)
	}
	tsEnd, err := ptypes.TimestampProto(end)
	if err != nil {
		log.Error(err)
		return status.Errorf(codes.Internal, "convert failed to convert end timestamp error: %s", err)
	}

	// we only look at first output and expect it to be a dataset URI
	inputs := produceRequest.GetInputs()
	if len(inputs) != 1 {
		log.Errorf("Expecting single input - produce request includes %d", len(inputs))
		return status.Errorf(codes.Internal, "Expecting single input - produce request includes %d", len(inputs))
	}

	// pull the dataset URI out of the produce request
	datasetURIValue, ok := inputs[0].GetValue().(*Value_DatasetUri)
	if !ok {
		inputType := reflect.TypeOf(inputs[0].GetValue())
		log.Errorf("Expecting Value_DatasetURI - produce input is %s", inputType)
		return status.Errorf(codes.Internal, "Expecting single input - produce request includes %s", inputType)
	}

	// pull the target and task out of the problem
	problemID, ok := s.pipelineProblemIDs[produceRequest.GetPipelineId()]
	if !ok {
		log.Errorf("Failed to find problemID for pipelineID: %s", produceRequest.GetPipelineId())
		return status.Errorf(codes.Internal, "no problem ID for pipelineID: %s", produceRequest.GetPipelineId())
	}
	problemRequest, ok := s.problemRequests[problemID]
	if !ok {
		log.Errorf("Failed to find problem request for problemID: %s", problemID)
		return status.Errorf(codes.Internal, "no problem request for problemID: %s", problemID)
	}
	taskType := problemRequest.GetProblem().GetProblem().GetTaskType()
	targetName := problemRequest.GetProblem().GetInputs()[0].GetTargets()[0].GetColumnName()

	// create mock result data
	resultURI, err := createResults(produceRequest.GetPipelineId(), datasetURIValue.DatasetUri, s.resultDir, targetName, taskType)
	if err != nil {
		log.Errorf("Failed to generate result data")
		return status.Errorf(codes.Internal, "Failed to generate result data")
	}
	exposedOutputs := map[string]*Value{
		"outputs.0": &Value{
			Value: &Value_DatasetUri{
				DatasetUri: resultURI,
			},
		},
	}

	// create a response emessage
	produceResults := &GetProducePipelineResultsResponse{
		Progress:       Progress_COMPLETED,
		Start:          tsStart,
		End:            tsEnd,
		ExposedOutputs: exposedOutputs,
	}

	// send it back to the caller
	err = stream.Send(produceResults)
	if err != nil {
		log.Error(err)
		return status.Errorf(codes.Internal, "failed to send produce result: %s", err)
	}
	return nil
}

// PipelineExport exports a previously generated pipeline.  The pipeline needs to have had a fit step
// executed on it to be valid for export.
func (s *Server) PipelineExport(ctx context.Context, req *PipelineExportRequest) (*PipelineExportResponse, error) {
	log.Infof("Received ExportPipeline - %v", req)

	// only allow produce on a pipeline that has had a fit run against it
	pipelineID := req.GetPipelineId()
	if !s.fitCompleteIDs.Has(pipelineID) {
		return nil, status.Errorf(codes.FailedPrecondition, "fit not executed for pipeline %s", pipelineID)
	}

	response := &PipelineExportResponse{}
	return response, nil
}

// ListPrimitives returns a list of TA1 primitives that TA3 is allowed to use in pre-processing pipeline
// specifications.
func (s *Server) ListPrimitives(ctx context.Context, req *ListPrimitivesRequest) (*ListPrimitivesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// StartSession creates a new session.
// TODO(jtorrez): implement this if it stays in MR, may not be in final API
func (s *Server) StartSession(ctx context.Context, req *StartSessionRequest) (*StartSessionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// EndSession ends an existing session.
// TODO(jtorrez): implement this if it stays in MR, may not be in final API
func (s *Server) EndSession(ctx context.Context, req *EndSessionRequest) (*EndSessionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// StartProblem creates a new problem instance.
// NOTE(cbethune): Placeholder because we need a way to get problem data into a search pipeline.  Once the
// status of this resolves we can remove or implement fully.
func (s *Server) StartProblem(ctx context.Context, req *StartProblemRequest) (*StartProblemResponse, error) {
	// store the problem ID
	problemID := uuid.NewV4().String()
	s.problemIDs.Add(problemID)
	s.problemRequests[problemID] = req

	response := &StartProblemResponse{
		ProblemId: problemID,
	}
	return response, nil
}

// UpdateProblem updates an existing problem instance.
func (s *Server) UpdateProblem(ctx context.Context, req *UpdateProblemRequest) (*UpdateProblemResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// EndProblem marks a problem as no longer used so that associated resources can be cleaned up.
// TODO(jtorrez): implement this if it stays in MR, may not be in final API
func (s *Server) EndProblem(ctx context.Context, req *EndProblemRequest) (*EndProblemResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}
