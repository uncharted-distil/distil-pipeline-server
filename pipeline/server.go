package pipeline

import (
	"fmt"
	"sync"
	"time"

	"github.com/fatih/set"
	"github.com/satori/go.uuid"
	"github.com/unchartedsoftware/plog"
	"golang.org/x/net/context"
)

const (
	sendDelay = 5 * time.Second
)

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

			// create a completed response

			scores := []*Score{}
			for _, metric := range request.GetMetric() {
				score := Score{
					Metric: metric,
					Value:  1.0,
				}
				scores = append(scores, &score)
			}

			info := &PipelineCreated{
				PredictResultUris: []string{"file://testdata/train_result.csv"},
				Output:            request.Output,
				Score:             scores,
			}
			completed := PipelineCreateResult{
				ResponseInfo: response,
				ProgressInfo: Progress_COMPLETE,
				PipelineId:   pipelineID,
				PipelineInfo: info,
			}
			results[2] = &completed

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
	s.sessionIDs.Add(id)
	response := createResponse(id, StatusCode_OK, "")
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
