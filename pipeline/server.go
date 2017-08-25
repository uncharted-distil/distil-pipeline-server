package pipeline

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
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

	// UserAgent is a string identifying this application
	UserAgent = "uncharted-ta2-test-server" // TODO: get a version tag embedded here
)

var version = versionUnset

// Version is the version of the TA3-TA2 API extracted from the protobuf definition.  Lazily evaluated
// since the protobuf init has to be complete before it can be used.  Doesn't change after initialization.
func Version() string {
	if version == versionUnset {
		version = getAPIVersion()
	}
	return version
}

// Server represents a basic distil pipeline server.
type Server struct {
	sessionIDs    *set.Set
	endSessionIDs *set.Set
	pipelineIDs   *set.Set
}

// NewServer creates a new pipeline server instance.  ID maps are initialized with place holder values
// to support tests without explicit calls to session management.
func NewServer() *Server {
	server := new(Server)
	server.sessionIDs = set.New("test-session-id")
	server.endSessionIDs = set.New("test-end-session-id")
	server.pipelineIDs = set.New("test-pipeline-id")
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
		err := stream.Send(errorPipelineCreateResult(StatusCode_SESSION_UNKNOWN, fmt.Sprintf("session %s does not exist", sessionID)))
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}
	if s.endSessionIDs.Has(sessionID) {
		log.Errorf("Session %s already closed", sessionID)
		err := stream.Send(errorPipelineCreateResult(StatusCode_SESSION_ENDED, fmt.Sprintf("session %s already ended", sessionID)))
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
			response := createResponse(StatusCode_OK, "")
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
			results = append(results, createPipelineResult(request, response, pipelineID, Progress_UPDATED))

			// create a completed response
			results = append(results, createPipelineResult(request, response, pipelineID, Progress_COMPLETED))

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
		result := errorPipelineExecuteResult(StatusCode_SESSION_UNKNOWN, fmt.Sprintf("session %s does not exist", sessionID))
		err := stream.Send(result)
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}
	if !s.endSessionIDs.Has(sessionID) {
		result := errorPipelineExecuteResult(StatusCode_SESSION_ENDED, fmt.Sprintf("session %s already ended", sessionID))
		err := stream.Send(result)
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}

	pipelineID := request.GetPipelineId()
	if !s.pipelineIDs.Has(pipelineID) {
		result := errorPipelineExecuteResult(StatusCode_INVALID_ARGUMENT, fmt.Sprintf("pipeline ID %s does not exist", pipelineID))
		err := stream.Send(result)
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}

	results := []*PipelineExecuteResult{}

	// create an initial submitted response
	response := createResponse(StatusCode_OK, "")
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
		ResponseInfo: createResponse(StatusCode_OK, ""),
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
	return createResponse(StatusCode_OK, ""), nil
}

// StartSession creates a new session
func (s *Server) StartSession(context context.Context, request *SessionRequest) (*SessionResponse, error) {
	id := uuid.NewV1().String()
	s.sessionIDs.Add(id)
	log.Infof("Received StartSession - API: [%s] User-Agent: [%s]", request.GetVersion(), request.GetUserAgent())

	if request.GetVersion() != Version() {
		log.Warnf("Client API version [%v] does not match expected version [%v]", request.GetVersion(), Version())
	}

	response := createSessionResponse(id, StatusCode_OK, "")
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
	response := createResponse(statusCode, responseStr)
	return response, nil
}

func createResponse(statusCode StatusCode, details string) *Response {
	response := &Response{
		Status: &Status{
			Code:    statusCode,
			Details: details,
		},
	}
	return response
}

func createSessionResponse(id string, statusCode StatusCode, details string) *SessionResponse {
	response := &SessionResponse{
		ResponseInfo: createResponse(statusCode, details),
		UserAgent:    "uncharted_test_agent_0_3",
		Version:      Version(),
		Context: &SessionContext{
			SessionId: id,
		},
	}
	return response
}

func errorPipelineCreateResult(status StatusCode, msg string) *PipelineCreateResult {
	response := createResponse(status, msg)
	return &PipelineCreateResult{ResponseInfo: response}
}

func errorPipelineExecuteResult(status StatusCode, msg string) *PipelineExecuteResult {
	response := createResponse(status, msg)
	return &PipelineExecuteResult{ResponseInfo: response}
}

func createPipelineResult(request *PipelineCreateRequest, response *Response, pipelineID string, progress Progress) *PipelineCreateResult {
	scores := []*Score{}
	for _, metric := range request.GetMetrics() {
		score := Score{
			Metric: metric,
			Value:  1.0,
		}
		scores = append(scores, &score)
	}

	pipeline := &Pipeline{
		PredictResultUris: []string{"file://testdata/train_result.csv"},
		Output:            request.GetOutput(),
		Scores:            scores,
	}

	return &PipelineCreateResult{
		ResponseInfo: response,
		ProgressInfo: progress,
		PipelineId:   pipelineID,
		PipelineInfo: pipeline,
	}
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
