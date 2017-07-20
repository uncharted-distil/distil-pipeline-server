package pipeline

import (
	"fmt"
	"time"

	"github.com/satori/go.uuid"
	log "github.com/unchartedsoftware/plog"
	"golang.org/x/net/context"
)

// Server represents a basic distil pipeline server.
type Server struct {
	sessionIDs map[string]interface{}
}

// CreatePipelines will create a mocked pipeline.
func (*Server) CreatePipelines(request *PipelineCreateRequest, stream PipelineCompute_CreatePipelinesServer) error {

	log.Info("Received CreatePipelines")

	id := uuid.NewV1().String()

	results := make([]*PipelineCreateResult, 3)

	// initial submitted response
	response := createResponse(id, StatusCode_OK, "")
	submitted := PipelineCreateResult{
		ResponseInfo: response,
		ProgressInfo: Progress_SUBMITTED,
		PipelineId:   id,
	}
	results = append(results, &submitted)

	// follow on running response
	running := PipelineCreateResult{
		ResponseInfo: response,
		ProgressInfo: Progress_RUNNING,
		PipelineId:   id,
	}
	results = append(results, &running)

	// completed response
	score := Score{
		Metric: Metric_F1_MACRO,
		Value:  1.0,
	}
	info := &PipelineCreated{
		PredictResultUris: []string{"file://data/result.json"},
		Output:            Output_GENERAL_SCORE,
		Score:             []*Score{&score},
	}

	completed := PipelineCreateResult{
		ResponseInfo: response,
		ProgressInfo: Progress_COMPLETE,
		PipelineId:   id,
		PipelineInfo: info,
	}
	results = append(results, &completed)

	// loop to send results every 5 seconds
	for i, result := range results {
		log.Infof("Sending part %d", i)
		if err := stream.Send(result); err != nil {
			return err
		}
		time.Sleep(5 * time.Second)
	}
	return nil
}

// ExecutePipeline sends a new pipeline execution request.
func (*Server) ExecutePipeline(*PipelineExecuteRequest, PipelineCompute_ExecutePipelineServer) error {
	return nil
}

// StartSession creates a new session
func (s *Server) StartSession(context.Context, *SessionRequest) (*Response, error) {
	log.Info("received start session request")
	id := uuid.NewV1().String()
	s.sessionIDs[id] = nil
	response := createResponse(id, StatusCode_OK, "")
	return response, nil
}

// EndSession closes and existing session
func (s *Server) EndSession(context context.Context, sessionContext *SessionContext) (*Response, error) {
	id := sessionContext.GetSessionId()
	responseStr := ""
	statusCode := StatusCode_OK
	if _, ok := s.sessionIDs[id]; !ok {
		responseStr = fmt.Sprintf("session %s does not exist", id)
		statusCode = StatusCode_INVALID_ARGUMENT
	}
	delete(s.sessionIDs, id)
	response := createResponse(id, statusCode, responseStr)
	return response, nil
}

// NewServer creates a new pipeline server instance
func NewServer() *Server {
	server := new(Server)
	server.sessionIDs = make(map[string]interface{})
	return server
}

func createResponse(id string, statusCode StatusCode, details string) *Response {
	return &Response{
		Context: &SessionContext{
			SessionId: id,
		},
		Status: &Status{
			Code:    statusCode,
			Details: details,
		},
	}
}
