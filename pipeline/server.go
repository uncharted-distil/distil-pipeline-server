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

	"github.com/pkg/errors"
	// data structures

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
	userAgent     string
	resultDir     string
	sendDelay     time.Duration
	numUpdates    int
	errPercentage float64
	maxPipelines  int

	sr *ServerRequests
}

// NewServer creates a new pipeline server instance.  ID maps are initialized with place holder values
// to support tests without explicit calls to session management.
func NewServer(userAgent string, resultDir string, sendDelay int,
	numUpdates int, errPercentage float64, maxPipelines int) *Server {
	server := new(Server)
	server.userAgent = userAgent
	server.resultDir = resultDir
	server.sendDelay = time.Duration(sendDelay) * time.Millisecond
	server.numUpdates = numUpdates
	server.errPercentage = errPercentage
	server.maxPipelines = maxPipelines

	server.sr = NewServerRequests()

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

// SearchPipelines generates a searchID and returns a SearchResponse immediately
func (s *Server) SearchPipelines(ctx context.Context, req *SearchPipelinesRequest) (*SearchPipelinesResponse, error) {
	log.Infof("Received SearchPipelines - %v", req)

	searchReq, err := s.sr.AddRequest(rootKey, req)
	if err != nil {
		return nil, handleError(codes.Internal, err)
	}

	// NOTE(jtorrez): could get fancy here and kick-off a goroutine that starts generating pipeline results
	// but leaving that out of first pass dummy results implementation,

	resp := &SearchPipelinesResponse{SearchId: searchReq.GetRequestID()}
	return resp, nil
}

// GetSearchPipelinesResults returns a stream of pipeline results associated with a previously issued request
func (s *Server) GetSearchPipelinesResults(req *GetSearchPipelinesResultsRequest, stream Core_GetSearchPipelinesResultsServer) error {
	log.Infof("Received GetSearchPipelinesResults - %v", req)

	searchID := req.GetSearchId()
	_, err := s.sr.GetRequest(searchID)
	if err != nil {
		return handleError(codes.InvalidArgument, err)
	}

	// randomly generate number of pipelines to "find"
	pipelinesFound := rand.Intn(s.maxPipelines) + 1

	wg := sync.WaitGroup{}
	wg.Add(pipelinesFound)

	// race condition is intentional - reporting last encountered error is sufficient
	var sendError error

	for i := 0; i < pipelinesFound; i++ {
		go func() {
			defer wg.Done()

			// Add a request node for the pipeline itself - it has no associated grpc request
			// object since it is spawned from the search
			pipelineReq, err := s.sr.AddRequest(req.GetSearchId(), nil)
			if err != nil {
				sendError = handleError(codes.Internal, err)
				return
			}

			resp := &GetSearchPipelinesResultsResponse{
				PipelineId: pipelineReq.GetRequestID(),
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
			sleepDuration := time.Duration(randomDelay)
			time.Sleep(sleepDuration)

			// mark the request as a complete
			s.sr.SetComplete(pipelineReq.GetRequestID())

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

// EndSearchPipelines Releases resources associated with a previusly issued search request.
// NOTE(cbethune): Does this require that a Stop request has been issued?
func (s *Server) EndSearchPipelines(ctx context.Context, req *EndSearchPipelinesRequest) (*EndSearchPipelinesResponse, error) {
	log.Infof("Received EndSearchPipelines - %v", req)
	searchID := req.GetSearchId()

	_, err := s.sr.GetRequest(searchID)
	if err != nil {
		return nil, handleError(codes.InvalidArgument, err)
	}

	err = s.sr.RemoveRequest(searchID)
	if err != nil {
		return nil, handleError(codes.Internal, err)
	}

	return &EndSearchPipelinesResponse{}, nil
}

// StopSearchPipelines Stops a running pipeline search request.
// NOTE(cbethune): Does this allow for a search to be restarted via a search request that uses the
// same ID?
func (s *Server) StopSearchPipelines(ctx context.Context, req *StopSearchPipelinesRequest) (*StopSearchPipelinesResponse, error) {
	log.Infof("Received StopSearchPipelines - %v", req)
	searchID := req.GetSearchId()
	_, err := s.sr.GetRequest(searchID)
	if err != nil {
		return nil, handleError(codes.InvalidArgument, err)
	}

	// mark the request as complete - score, fit, produce and still execute
	s.sr.SetComplete(searchID)

	return &StopSearchPipelinesResponse{}, nil
}

// DescribePipeline generates a pipeline description struct for a given pipeline.
func (s *Server) DescribePipeline(ctx context.Context, req *DescribePipelineRequest) (*DescribePipelineResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// ScorePipeline generates a score for a given pipeline.
func (s *Server) ScorePipeline(ctx context.Context, req *ScorePipelineRequest) (*ScorePipelineResponse, error) {
	log.Infof("Received ScorePipeline - %v", req)

	pipelineID := req.GetPipelineId()
	scoreRequest, err := s.sr.AddRequest(pipelineID, req)
	if err != nil {
		return nil, handleError(codes.InvalidArgument, err)
	}

	response := &ScorePipelineResponse{scoreRequest.GetRequestID()}
	return response, err
}

// GetScorePipelineResults returns a stream of pipeline score results for a previously issued  scoring request.
func (s *Server) GetScorePipelineResults(req *GetScorePipelineResultsRequest, stream Core_GetScorePipelineResultsServer) error {
	log.Infof("Received GetScorePipelineResults - %v", req)
	scoreID := req.GetRequestId()

	scoreRequest, err := s.sr.GetRequest(scoreID)
	if err != nil {
		return handleError(codes.InvalidArgument, err)
	}

	scoreMsg, ok := scoreRequest.GetRequestMsg().(*ScorePipelineRequest)
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
				Value: &Value_Double{rand.Float64()},
			},
		})
	}

	// sleep for a bit
	start := time.Now()
	randomDelay := rand.Intn(int(s.sendDelay))
	sleepDuration := time.Duration(randomDelay)
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
	scoreResult := &GetScorePipelineResultsResponse{
		Progress: Progress_COMPLETED,
		Start:    tsStart,
		End:      tsEnd,
		Scores:   scores,
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

// FitPipeline fits a pipeline to training data.
func (s *Server) FitPipeline(ctx context.Context, req *FitPipelineRequest) (*FitPipelineResponse, error) {
	log.Infof("Received FitPipeline - %v", req)

	pipelineID := req.GetPipelineId()
	fitRequest, err := s.sr.AddRequest(pipelineID, req)
	if err != nil {
		return nil, handleError(codes.InvalidArgument, err)
	}

	// save the request ID and mark the fit as incomplete (produce can't execute on an incomplete fit)
	response := &FitPipelineResponse{fitRequest.GetRequestID()}
	return response, nil
}

// GetFitPipelineResults returns a stream of pipeline fit result for a previously issued fit request.
func (s *Server) GetFitPipelineResults(req *GetFitPipelineResultsRequest, stream Core_GetFitPipelineResultsServer) error {
	log.Infof("Received GetFitPipelineResults - %v", req)

	fitID := req.GetRequestId()
	_, err := s.sr.GetRequest(fitID)
	if err != nil {
		return handleError(codes.InvalidArgument, err)
	}

	// apply a random delay
	start := time.Now()
	randomDelay := rand.Intn(int(s.sendDelay))
	sleepDuration := time.Duration(randomDelay)
	time.Sleep(sleepDuration)
	end := time.Now()

	// mark the fit for the pipeline as complete
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

	// create the response message
	fitResult := &GetFitPipelineResultsResponse{
		Progress: Progress_COMPLETED,
		Start:    tsStart,
		End:      tsEnd,
	}

	// send it back to the caller
	err = stream.Send(fitResult)
	if err != nil {
		return handleError(codes.Internal, err)
	}

	return nil
}

func (s *Server) checkPipelineFit(pipelineID string) (bool, error) {
	fitComplete := false
	node, err := s.sr.GetRequest(pipelineID)
	if err != nil {
		return false, handleError(codes.Internal, err)
	}
	for _, childID := range node.GetChildren() {
		child, err := s.sr.GetRequest(childID)
		if err != nil {
			return false, handleError(codes.Internal, err)
		}
		_, ok := child.GetRequestMsg().(*FitPipelineRequest)
		if ok && s.sr.IsComplete(child.GetRequestID()) {
			fitComplete = true
			break
		}
	}
	return fitComplete, nil
}

// ProducePipeline executes a pipeline on supplied data.  Pipeline needs to have previously executed a fit.
func (s *Server) ProducePipeline(ctx context.Context, req *ProducePipelineRequest) (*ProducePipelineResponse, error) {
	log.Infof("Received ProducePipeline - %v", req)
	pipelineID := req.GetPipelineId()

	produceRequest, err := s.sr.AddRequest(pipelineID, req)
	if err != nil {
		return nil, handleError(codes.InvalidArgument, err)
	}

	// check to see if a fit has been performed on the associated pipeline
	fitComplete, err := s.checkPipelineFit(produceRequest.GetParent())
	if err != nil {
		return nil, handleError(codes.Internal, err)
	}
	if !fitComplete {
		return nil, handleError(codes.FailedPrecondition, errors.Errorf("no fit executed on pipeline `%s`", pipelineID))
	}

	response := &ProducePipelineResponse{produceRequest.GetRequestID()}
	return response, nil
}

// GetProducePipelineResults returns a stream of pipeline results for a previously issued produce request.
func (s *Server) GetProducePipelineResults(req *GetProducePipelineResultsRequest, stream Core_GetProducePipelineResultsServer) error {
	log.Infof("Received GetProducePipelineResults - %v", req)
	produceID := req.GetRequestId()

	produceRequest, err := s.sr.GetRequest(produceID)
	if err != nil {
		return handleError(codes.Internal, err)
	}

	// apply a random delay
	start := time.Now()
	randomDelay := rand.Intn(int(s.sendDelay))
	sleepDuration := time.Duration(randomDelay)
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

	produceRequestMsg, ok := produceRequest.GetRequestMsg().(*ProducePipelineRequest)
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
	var problem *ProblemDescription
	for parentID != "" {
		parentRequest, err := s.sr.GetRequest(parentID)
		if err != nil {
			return handleError(codes.Internal, err)
		}
		searchRequestMsg, ok := parentRequest.GetRequestMsg().(*SearchPipelinesRequest)
		if ok {
			problem = searchRequestMsg.GetProblem()
			break
		}
		parentID = parentRequest.GetParent()
	}

	taskType := problem.GetProblem().GetTaskType()

	problemInputs := problem.GetInputs()
	if len(problemInputs) != 1 {
		return handleError(codes.Internal, errors.Errorf("expecting single input in problem, found %d", len(problemInputs)))
	}

	problemTargets := problemInputs[0].GetTargets()
	if len(problemInputs) != 1 {
		return handleError(codes.Internal, errors.Errorf("expecting single target in problem, found %d", len(problemTargets)))
	}

	targetName := problemTargets[0].GetColumnName()

	// create mock result data
	resultURI, err := createResults(produceRequestMsg.GetPipelineId(), datasetURIValue.DatasetUri, s.resultDir, targetName, taskType)
	if err != nil {
		return handleError(codes.Internal, errors.Errorf("Failed to generate result data for pipeline `%s`", produceRequestMsg.GetPipelineId()))
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
		handleError(codes.Internal, err)
	}
	return nil
}

// PipelineExport exports a previously generated pipeline.  The pipeline needs to have had a fit step
// executed on it to be valid for export.
func (s *Server) PipelineExport(ctx context.Context, req *PipelineExportRequest) (*PipelineExportResponse, error) {
	log.Infof("Received ExportPipeline - %v", req)

	pipelineID := req.GetPipelineId()
	_, err := s.sr.GetRequest(pipelineID)
	if err != nil {
		handleError(codes.InvalidArgument, err)
	}
	// only allow produce on a pipeline that has had a fit run against it
	fitComplete, err := s.checkPipelineFit(pipelineID)
	if err != nil {
		handleError(codes.Internal, err)
	}
	if !fitComplete {
		return nil, handleError(codes.FailedPrecondition, errors.Errorf("no fit executed on pipeline `%s`", pipelineID))
	}

	response := &PipelineExportResponse{}
	return response, nil
}

// UpdateProblem updates the problem defintion associated with a search
func (s *Server) UpdateProblem(ctx context.Context, req *UpdateProblemRequest) (*UpdateProblemResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// ListPrimitives returns a list of TA1 primitives that TA3 is allowed to use in pre-processing pipeline
// specifications.
func (s *Server) ListPrimitives(ctx context.Context, req *ListPrimitivesRequest) (*ListPrimitivesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// ListAllowedValueTypes returns a list of all value types supported by this TA2 system to the caller
func (s *Server) ListAllowedValueTypes(ctx context.Context, req *ListAllowedValueTypesRequest) (*ListAllowedValueTypesResponse, error) {
	response := &ListAllowedValueTypesResponse{
		AllowedValueTypes: []ValueType{ValueType_DATASET_URI},
	}
	return response, nil
}
