package pipeline

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/fatih/set"
	"github.com/golang/protobuf/proto"
	protobuf "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/satori/go.uuid"
	"github.com/unchartedsoftware/plog"
	"golang.org/x/net/context"
)

const (
	sendDelay    = 5 * time.Second
	versionUnset = "version_unset"
	resultPath   = "results"
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

// Server represents a basic distil pipeline server.
type Server struct {
	sessionIDs    *set.Set
	endSessionIDs *set.Set
	pipelineIDs   *set.Set
	userAgent     string
}

// NewServer creates a new pipeline server instance.  ID maps are initialized with place holder values
// to support tests without explicit calls to session management.
func NewServer(userAgent string) *Server {
	server := new(Server)
	server.sessionIDs = set.New("test-session-id")
	server.endSessionIDs = set.New("test-end-session-id")
	server.pipelineIDs = set.New("test-pipeline-id")
	server.userAgent = userAgent
	return server
}

// CreatePipelines will create a mocked pipeline.
func (s *Server) CreatePipelines(request *PipelineCreateRequest, stream PipelineCompute_CreatePipelinesServer) error {

	log.Infof("Received CreatePipelines - %v", request)

	// If the session ID doesn't exist return a single result flagging the error
	// and close the stream.
	sessionID := request.Context.GetSessionId()
	if !s.sessionIDs.Has(sessionID) {
		log.Errorf("Session %s does not exist", sessionID)
		err := stream.Send(newPipelineCreateResult(StatusCode_SESSION_UNKNOWN, fmt.Sprintf("session %s does not exist", sessionID)))
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}
	if s.endSessionIDs.Has(sessionID) {
		log.Errorf("Session %s already closed", sessionID)
		err := stream.Send(newPipelineCreateResult(StatusCode_SESSION_ENDED, fmt.Sprintf("session %s already ended", sessionID)))
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(int(request.GetMaxPipelines()))

	// race condition is intentional - reporting last encountered error is sufficient
	var sendError error

	for i := int32(0); i < request.GetMaxPipelines(); i++ {
		go func() {
			pipelineID := uuid.NewV1().String()

			// save the pipeline ID for subsequent calls
			s.pipelineIDs.Add(pipelineID)

			results := []*PipelineCreateResult{}

			// create an initial submitted response
			response := newResponse(StatusCode_OK, "")
			submitted := PipelineCreateResult{
				ResponseInfo: response,
				ProgressInfo: Progress_SUBMITTED,
				PipelineId:   pipelineID,
			}
			results = append(results, &submitted)

			// create a follow on running response
			running := PipelineCreateResult{
				ResponseInfo: response,
				ProgressInfo: Progress_RUNNING,
				PipelineId:   pipelineID,
			}
			results = append(results, &running)

			// create an updated response
			updated, err := createPipelineResult(request, response, pipelineID, Progress_UPDATED)
			if err != nil {
				sendError = err
				return
			}
			results = append(results, updated)

			// create a completed response
			completed, err := createPipelineResult(request, response, pipelineID, Progress_COMPLETED)
			if err != nil {
				sendError = err
				return
			}
			results = append(results, completed)

			// Loop to send results every n seconds.
			for i, result := range results {
				log.Infof("Sending part %d", i)
				if err := stream.Send(result); err != nil {
					log.Error(err)
					sendError = err
				}
				time.Sleep(sendDelay)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	return sendError
}

// ExecutePipeline mocks a pipeline execution.
func (s *Server) ExecutePipeline(request *PipelineExecuteRequest, stream PipelineCompute_ExecutePipelineServer) error {
	log.Infof("Received ExecutePipeline - %v", request)

	// If the session ID doesn't exist return a single result flagging the error
	// and close the stream.
	sessionID := request.Context.GetSessionId()
	if !s.sessionIDs.Has(sessionID) {
		result := newPipelineExecuteResult(StatusCode_SESSION_UNKNOWN, fmt.Sprintf("session %s does not exist", sessionID))
		err := stream.Send(result)
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}
	if !s.endSessionIDs.Has(sessionID) {
		result := newPipelineExecuteResult(StatusCode_SESSION_ENDED, fmt.Sprintf("session %s already ended", sessionID))
		err := stream.Send(result)
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}

	pipelineID := request.GetPipelineId()
	if !s.pipelineIDs.Has(pipelineID) {
		result := newPipelineExecuteResult(StatusCode_INVALID_ARGUMENT, fmt.Sprintf("pipeline ID %s does not exist", pipelineID))
		err := stream.Send(result)
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}

	results := []*PipelineExecuteResult{}

	// create an initial submitted response
	response := newResponse(StatusCode_OK, "")
	submitted := PipelineExecuteResult{
		ResponseInfo: response,
		ProgressInfo: Progress_SUBMITTED,
		PipelineId:   pipelineID,
	}
	results = append(results, &submitted)

	// create a follow on running response
	running := PipelineExecuteResult{
		ResponseInfo: response,
		ProgressInfo: Progress_RUNNING,
		PipelineId:   pipelineID,
	}
	results = append(results, &running)

	// create a completed response
	completed := PipelineExecuteResult{
		ResponseInfo: response,
		ProgressInfo: Progress_COMPLETED,
		PipelineId:   pipelineID,
		ResultUris:   []string{"file://testdata/predict_result.csv"},
	}
	results = append(results, &completed)

	// loop to send results with delay
	for i, result := range results {
		log.Infof("Sending part %d", i)
		if err := stream.Send(result); err != nil {
			log.Error(err)
			return err
		}
		time.Sleep(sendDelay)
	}
	return nil
}

// ListPipelines lists actively running pipelines for a session
func (s *Server) ListPipelines(context context.Context, request *PipelineListRequest) (*PipelineListResult, error) {
	result := &PipelineListResult{
		ResponseInfo: newResponse(StatusCode_OK, ""),
		PipelineIds:  set.StringSlice(s.pipelineIDs),
	}
	return result, nil
}

// GetCreatePipelineResults fetches create pipeline results for an actively running pipeline session
func (s *Server) GetCreatePipelineResults(request *PipelineCreateResultsRequest, stream PipelineCompute_GetCreatePipelineResultsServer) error {
	log.Warn("Not implemented")
	return nil
}

// GetExecutePipelineResults fetches create pipeline results for an actively running pipeline session
func (s *Server) GetExecutePipelineResults(request *PipelineExecuteResultsRequest, stream PipelineCompute_GetExecutePipelineResultsServer) error {
	log.Warn("Not implemented")
	return nil
}

// UpdateProblemSchema modfies the TA2 server's understanding of the problem schema
func (s *Server) UpdateProblemSchema(context context.Context, request *UpdateProblemSchemaRequest) (*Response, error) {
	return newResponse(StatusCode_OK, ""), nil
}

// StartSession creates a new session
func (s *Server) StartSession(context context.Context, request *SessionRequest) (*SessionResponse, error) {
	id := uuid.NewV1().String()
	s.sessionIDs.Add(id)
	log.Infof("Received StartSession - API: [%s] User-Agent: [%s]", request.GetVersion(), request.GetUserAgent())

	if request.GetVersion() != APIVersion() {
		log.Warnf("Client API version [%v] does not match expected version [%v]", request.GetVersion(), APIVersion())
	}

	response := newSessionResponse(id, StatusCode_OK, "", s.userAgent, APIVersion())
	return response, nil
}

// EndSession closes and existing session
func (s *Server) EndSession(context context.Context, sessionContext *SessionContext) (*Response, error) {
	log.Infof("Received EndSession - %v", sessionContext)
	id := sessionContext.GetSessionId()
	responseStr := ""
	statusCode := StatusCode_OK
	if !s.sessionIDs.Has(id) {
		responseStr = fmt.Sprintf("session %s does not exist", id)
		statusCode = StatusCode_SESSION_UNKNOWN
	} else if s.endSessionIDs.Has(id) {
		responseStr = fmt.Sprintf("session %s already ended", id)
		statusCode = StatusCode_SESSION_ENDED
	}
	s.endSessionIDs.Add(id)
	s.sessionIDs.Remove(id)
	response := newResponse(statusCode, responseStr)
	return response, nil
}

func newResponse(statusCode StatusCode, details string) *Response {
	response := &Response{
		Status: &Status{
			Code:    statusCode,
			Details: details,
		},
	}
	return response
}

func newSessionResponse(id string, statusCode StatusCode, details string, userAgent string, version string) *SessionResponse {
	response := &SessionResponse{
		ResponseInfo: newResponse(statusCode, details),
		UserAgent:    userAgent,
		Version:      version,
		Context: &SessionContext{
			SessionId: id,
		},
	}
	return response
}

func newPipelineCreateResult(status StatusCode, msg string) *PipelineCreateResult {
	response := newResponse(status, msg)
	return &PipelineCreateResult{ResponseInfo: response}
}

func newPipelineExecuteResult(status StatusCode, msg string) *PipelineExecuteResult {
	response := newResponse(status, msg)
	return &PipelineExecuteResult{ResponseInfo: response}
}

func createPipelineResult(request *PipelineCreateRequest, response *Response, pipelineID string, progress Progress) (*PipelineCreateResult, error) {
	scores := []*Score{}
	for _, metric := range request.GetMetrics() {
		score := Score{
			Metric: metric,
			Value:  1.0,
		}
		scores = append(scores, &score)
	}

	// create stub data generators based on task
	var generator func() string
	if request.GetTask() == TaskType_CLASSIFICATION {
		generator = func() string {
			if rand.Float32() > 0.5 {
				return "1"
			}
			return "0"
		}
	} else if request.GetTask() == TaskType_REGRESSION {
		generator = func() string {
			return strconv.FormatFloat(rand.Float64(), 'f', 4, 64)
		}
	} else {
		err := fmt.Errorf("unhandled task type %s", request.GetTask())
		log.Error(err)
		return nil, err
	}

	// generate and persist mock result csv
	trainPath := request.GetTrainFeatures()[0].GetDataUri()
	targetFeature := request.GetTargetFeatures()[0].GetFeatureId()
	resultDir, err := generateResultCsv(pipelineID, trainPath, resultPath, targetFeature, generator)
	if err != nil {
		log.Errorf("Failed to generate results: %s", err)
		return nil, err
	}

	resultPath, err := filepath.Abs(resultDir)
	if err != nil {
		log.Errorf("Failed to generate absolute path: %s", err)
		return nil, err
	}

	pipeline := &Pipeline{
		PredictResultUris: []string{resultPath},
		Output:            request.GetOutput(),
		Scores:            scores,
	}

	return &PipelineCreateResult{
		ResponseInfo: response,
		ProgressInfo: progress,
		PipelineId:   pipelineID,
		PipelineInfo: pipeline,
	}, nil
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
