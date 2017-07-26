package pipeline

import (
	"fmt"
	"time"

	"github.com/satori/go.uuid"
	"github.com/unchartedsoftware/plog"
	"golang.org/x/net/context"
)

const (
	sendDelay = 5 * time.Second
)

// Server represents a basic distil pipeline server.
type Server struct {
	sessionIDs    map[string]interface{}
	endSessionIDs map[string]interface{}
	pipelineIDs   map[string]interface{}
}

// NewServer creates a new pipeline server instance.  ID maps are initialized with place holder values
// to support tests without explicit calls to session management.
func NewServer() *Server {
	server := new(Server)
	server.sessionIDs = map[string]interface{}{"test-session-id": nil}
	server.endSessionIDs = map[string]interface{}{"test-end-session-id": nil}
	server.pipelineIDs = map[string]interface{}{"test-pipeline-id": nil}
	return server
}

// CreatePipelines will create a mocked pipeline.
func (s *Server) CreatePipelines(request *PipelineCreateRequest, stream PipelineCompute_CreatePipelinesServer) error {

	log.Infof("Received CreatePipelines - %v", request)

	pipelineID := uuid.NewV1().String()

	// If the session ID doesn't exist return a single result flagging the error
	// and close the stream.
	sessionID := request.Context.GetSessionId()
	if _, ok := s.sessionIDs[sessionID]; !ok {
		log.Errorf("Session %s does not exist", sessionID)
		err := stream.Send(errorPipelineCreateResult(StatusCode_SESSION_UNKNOWN, fmt.Sprintf("session %s does not exist", sessionID)))
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}
	if _, ok := s.endSessionIDs[sessionID]; ok {
		log.Errorf("Session %s already closed", sessionID)
		err := stream.Send(errorPipelineCreateResult(StatusCode_SESSION_ENDED, fmt.Sprintf("session %s already ended", sessionID)))
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}

	// save the pipeline ID for subsequent calls
	s.pipelineIDs[pipelineID] = nil

	results := make([]*PipelineCreateResult, 3)

	// create an initial submitted response
	response := createResponse(pipelineID, StatusCode_OK, "")
	submitted := PipelineCreateResult{
		ResponseInfo: response,
		ProgressInfo: Progress_SUBMITTED,
		PipelineId:   pipelineID,
	}
	results[0] = &submitted

	// create a follow on running response
	running := PipelineCreateResult{
		ResponseInfo: response,
		ProgressInfo: Progress_RUNNING,
		PipelineId:   pipelineID,
	}
	results[1] = &running

	// completed response
	score := Score{
		Metric: Metric_F1_MACRO,
		Value:  1.0,
	}
	info := &PipelineCreated{
		PredictResultUris: []string{"file://testdata/train_result.csv"},
		Output:            Output_GENERAL_SCORE,
		Score:             []*Score{&score},
	}
	completed := PipelineCreateResult{
		ResponseInfo: response,
		ProgressInfo: Progress_COMPLETE,
		PipelineId:   pipelineID,
		PipelineInfo: info,
	}
	results[2] = &completed

	// loop to send results every 5 seconds
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

// ExecutePipeline sends a new pipeline execution request.
func (s *Server) ExecutePipeline(request *PipelineExecuteRequest, stream PipelineCompute_ExecutePipelineServer) error {
	log.Infof("Received ExecutePipeline - %v", request)

	// If the session ID doesn't exist return a single result flagging the error
	// and close the stream.
	sessionID := request.Context.GetSessionId()
	if _, ok := s.sessionIDs[sessionID]; !ok {
		result := errorPipelineExecuteResult(StatusCode_SESSION_UNKNOWN, fmt.Sprintf("session %s does not exist", sessionID))
		err := stream.Send(result)
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}
	if _, ok := s.endSessionIDs[sessionID]; ok {
		result := errorPipelineExecuteResult(StatusCode_SESSION_ENDED, fmt.Sprintf("session %s already ended", sessionID))
		err := stream.Send(result)
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}

	pipelineID := request.GetPipelineId()
	if _, ok := s.pipelineIDs[pipelineID]; !ok {
		result := errorPipelineExecuteResult(StatusCode_INVALID_ARGUMENT, fmt.Sprintf("pipeline ID %s does not exist", pipelineID))
		err := stream.Send(result)
		if err != nil {
			log.Error(err)
			return err
		}
		return nil
	}

	results := make([]*PipelineExecuteResult, 3)

	// create an initial submitted response
	response := createResponse(sessionID, StatusCode_OK, "")
	submitted := PipelineExecuteResult{
		ResponseInfo: response,
		ProgressInfo: Progress_SUBMITTED,
		PipelineId:   pipelineID,
	}
	results[0] = &submitted

	// create a follow on running response
	running := PipelineExecuteResult{
		ResponseInfo: response,
		ProgressInfo: Progress_RUNNING,
		PipelineId:   pipelineID,
	}
	results[1] = &running

	completed := PipelineExecuteResult{
		ResponseInfo: response,
		ProgressInfo: Progress_COMPLETE,
		PipelineId:   pipelineID,
		ResultUris:   []string{"file://testdata/predict_result.csv"},
	}
	results[2] = &completed

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

// StartSession creates a new session
func (s *Server) StartSession(context.Context, *SessionRequest) (*Response, error) {
	log.Info("Received StartSession")
	id := uuid.NewV1().String()
	s.sessionIDs[id] = nil
	response := createResponse(id, StatusCode_OK, "")
	return response, nil
}

// EndSession closes and existing session
func (s *Server) EndSession(context context.Context, sessionContext *SessionContext) (*Response, error) {
	log.Infof("Received EndSession - %v", sessionContext)
	id := sessionContext.GetSessionId()
	responseStr := ""
	statusCode := StatusCode_OK
	if _, ok := s.sessionIDs[id]; !ok {
		responseStr = fmt.Sprintf("session %s does not exist", id)
		statusCode = StatusCode_SESSION_UNKNOWN
	} else if _, ok := s.endSessionIDs[id]; ok {
		responseStr = fmt.Sprintf("session %s already ended", id)
		statusCode = StatusCode_SESSION_ENDED
	}
	s.endSessionIDs[id] = nil
	delete(s.sessionIDs, id)
	response := createResponse(id, statusCode, responseStr)
	return response, nil
}

func createResponse(id string, statusCode StatusCode, details string) *Response {
	response := &Response{
		Context: &SessionContext{
			SessionId: id,
		},
		Status: &Status{
			Code:    statusCode,
			Details: details,
		},
	}
	return response
}

func errorPipelineCreateResult(status StatusCode, msg string) *PipelineCreateResult {
	response := createResponse("", status, msg)
	return &PipelineCreateResult{ResponseInfo: response}
}

func errorPipelineExecuteResult(status StatusCode, msg string) *PipelineExecuteResult {
	response := createResponse("", status, msg)
	return &PipelineExecuteResult{ResponseInfo: response}
}
