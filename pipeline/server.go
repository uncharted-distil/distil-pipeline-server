package pipeline

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
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

// Server represents a basic distil pipeline server.
type Server struct {
	sessionIDs    *set.Set
	endSessionIDs *set.Set
	pipelineIDs   *set.Set
	userAgent     string
	resultDir     string
	sendDelay     time.Duration
}

// StatusErr provides an status code and an error message
type StatusErr struct {
	Status   StatusCode
	ErrorMsg string
}

func (s *StatusErr) Error() string {
	return s.ErrorMsg
}

// NewServer creates a new pipeline server instance.  ID maps are initialized with place holder values
// to support tests without explicit calls to session management.
func NewServer(userAgent string, resultDir string, sendDelay int64) *Server {
	server := new(Server)
	server.sessionIDs = set.New("test-session-id")
	server.endSessionIDs = set.New("test-end-session-id")
	server.pipelineIDs = set.New("test-pipeline-id")
	server.userAgent = userAgent
	server.resultDir = resultDir
	server.sendDelay = time.Duration(sendDelay) * time.Millisecond
	return server
}

// CreatePipelines will create a mocked pipeline.
func (s *Server) CreatePipelines(request *PipelineCreateRequest, stream Core_CreatePipelinesServer) error {

	log.Infof("Received CreatePipelines - %v", request)

	// If the session ID doesn't exist return a single result flagging the error
	// and close the stream.
	sessionID := request.Context.GetSessionId()
	err := s.validateSession(sessionID)
	if err != nil {
		log.Error(err.Error())
		err := stream.Send(newPipelineCreateResult(err.Status, err.Error()))
		if err != nil {
			log.Error(err.Error())
		}
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(int(request.GetMaxPipelines()))

	// race condition is intentional - reporting last encountered error is sufficient
	var sendError error

	for i := int32(0); i < request.GetMaxPipelines(); i++ {
		go func() {
			defer wg.Done()

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
			updated, err := createPipelineResult(request, response, pipelineID, Progress_UPDATED, 0, s.resultDir)
			if err != nil {
				sendError = err
				return
			}
			results = append(results, updated)

			// create a completed response
			completed, err := createPipelineResult(request, response, pipelineID, Progress_COMPLETED, 1, s.resultDir)
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
				time.Sleep(s.sendDelay)
			}
		}()
	}
	wg.Wait()

	return sendError
}

// ExecutePipeline mocks a pipeline execution.
func (s *Server) ExecutePipeline(request *PipelineExecuteRequest, stream Core_ExecutePipelineServer) error {
	log.Infof("Received ExecutePipeline - %v", request)

	// If the session ID doesn't exist return a single result flagging the error
	// and close the stream.
	sessionID := request.Context.GetSessionId()
	err := s.validateSession(sessionID)
	if err != nil {
		log.Error(err.Error())
		err := stream.Send(newPipelineExecuteResult(err.Status, err.Error()))
		if err != nil {
			log.Error(err.Error())
		}
		return err
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
		ResultUri:    "file://testdata/predict_result.csv",
	}
	results = append(results, &completed)

	// loop to send results with delay
	for i, result := range results {
		log.Infof("Sending part %d", i)
		if err := stream.Send(result); err != nil {
			log.Error(err)
			return err
		}
		time.Sleep(s.sendDelay)
	}
	return nil
}

// ListPipelines lists actively running pipelines for a session
func (s *Server) ListPipelines(context context.Context, request *PipelineListRequest) (*PipelineListResult, error) {
	var response *Response
	sessionID := request.Context.GetSessionId()
	err := s.validateSession(sessionID)
	if err != nil {
		log.Error(err.Error())
		response = newResponse(err.Status, err.Error())
	} else {
		response = newResponse(StatusCode_OK, "")
	}

	result := &PipelineListResult{
		ResponseInfo: response,
		PipelineIds:  set.StringSlice(s.pipelineIDs),
	}
	return result, nil
}

// DeletePipelines deletes a set of running pipelines
func (s *Server) DeletePipelines(context context.Context, request *PipelineDeleteRequest) (*PipelineListResult, error) {
	var response *Response
	sessionID := request.Context.GetSessionId()
	err := s.validateSession(sessionID)
	if err != nil {
		log.Error(err.Error())
		response = newResponse(err.Status, err.Error())
	} else {
		response = newResponse(StatusCode_OK, "")
	}

	// add any that are currently running to the delete list
	deleted := []string{}
	for _, id := range request.DeletePipelineIds {
		if s.pipelineIDs.Has(id) {
			deleted = append(deleted, id)
		}
	}

	result := &PipelineListResult{
		ResponseInfo: response,
		PipelineIds:  deleted,
	}
	return result, nil
}

// CancelPipelines - stops a pipeline from running.  Not yet implemented.
func (s *Server) CancelPipelines(context context.Context, request *PipelineCancelRequest) (*PipelineListResult, error) {
	response := newResponse(StatusCode_UNIMPLEMENTED, "method not implemented")
	result := &PipelineListResult{
		ResponseInfo: response,
		PipelineIds:  nil,
	}
	return result, nil
}

// ExportPipeline request that the TA2 system export the current pipeline
func (s *Server) ExportPipeline(contex context.Context, request *PipelineExportRequest) (*Response, error) {
	sessionID := request.Context.GetSessionId()
	err := s.validateSession(sessionID)
	if err != nil {
		log.Error(err.Error())
		return newResponse(err.Status, err.Error()), nil
	}
	return newResponse(StatusCode_OK, ""), nil
}

// GetCreatePipelineResults fetches create pipeline results for an actively running pipeline session
func (s *Server) GetCreatePipelineResults(request *PipelineCreateResultsRequest, stream Core_GetCreatePipelineResultsServer) error {
	log.Warn("Not implemented")
	return nil
}

// GetExecutePipelineResults fetches create pipeline results for an actively running pipeline session
func (s *Server) GetExecutePipelineResults(request *PipelineExecuteResultsRequest, stream Core_GetExecutePipelineResultsServer) error {
	log.Warn("Not implemented")
	return nil
}

// SetProblemDoc modfies the TA2 server's understanding of the problem schema
func (s *Server) SetProblemDoc(context context.Context, request *SetProblemDocRequest) (*Response, error) {
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

func createPipelineResult(
	request *PipelineCreateRequest,
	response *Response,
	pipelineID string,
	progress Progress,
	seqNum int,
	resultPath string,
) (*PipelineCreateResult, error) {
	scores := []*Score{}
	for _, metric := range request.GetMetrics() {
		score := Score{
			Metric: metric,
			Value:  1.0,
		}
		scores = append(scores, &score)
	}

	// path to dataset root
	dataPath := request.GetDatasetUri()
	dataPath = strings.Replace(dataPath, "file://", "", 1)

	targetFeature := request.GetTargetFeatures()[0].GetFeatureName()

	targetLookup, err := buildLookup(dataPath, targetFeature)
	if err != nil {
		return nil, err
	}

	// create stub data generators based on task
	var generator func(int) string
	if request.GetTask() == TaskType_CLASSIFICATION {
		cats, err := getCategories(dataPath, targetFeature)
		if err != nil {
			log.Errorf("Error generating data: %v", err)
			return nil, err
		}

		generator = func(index int) string {
			if rand.Float32() > 0.9 {
				return cats[rand.Intn(len(cats))]
			}

			return targetLookup[fmt.Sprintf("%d", index)]
		}
	} else if request.GetTask() == TaskType_REGRESSION {
		generator = func(index int) string {
			var desiredMean float64
			targetValue := targetLookup[fmt.Sprintf("%d", index)]
			if targetValue != "" {
				desiredMean, err = strconv.ParseFloat(targetValue, 64)
				if err != nil {
					log.Errorf("Error generating data: %v", err)
					// TODO: use min & max values and randomly pick a value in between.
					return strconv.FormatFloat(rand.Float64(), 'f', 4, 64)
				}
			}

			adjustment := rand.Float64() * 0.1
			value := adjustment*desiredMean + desiredMean
			return strconv.FormatFloat(value, 'f', 4, 64)
		}
	} else {
		err := fmt.Errorf("unhandled task type %s", request.GetTask())
		log.Error(err)
		return nil, err
	}

	// generate and persist mock result csv
	resultDir, err := generateResultCsv(pipelineID, seqNum, dataPath, resultPath, targetFeature, generator)
	if err != nil {
		log.Errorf("Failed to generate results: %s", err)
		return nil, err
	}

	absResultDir, err := filepath.Abs(resultDir)
	if err != nil {
		log.Errorf("Failed to generate absolute path: %s", err)
		return nil, err
	}

	pipeline := &Pipeline{
		PredictResultUri: absResultDir,
		Output:           request.GetOutput(),
		Scores:           scores,
	}

	return &PipelineCreateResult{
		ResponseInfo: response,
		ProgressInfo: progress,
		PipelineId:   pipelineID,
		PipelineInfo: pipeline,
	}, nil
}

func buildLookup(csvPath string, fieldName string) (map[string]string, error) {
	// Load the data
	data, err := loadDataCsv(csvPath)
	if err != nil {
		return nil, err
	}

	// Map the field name to an index.
	var fieldIndex = -1
	for i, field := range data[0] {
		if fieldName == field {
			fieldIndex = i
		}
	}

	// Map the index to the target value.
	// Assume the index is the first field.
	lookup := make(map[string]string)
	for _, row := range data[1:] {
		lookup[row[0]] = row[fieldIndex]
	}

	return lookup, nil
}

func getCategories(csvPath string, fieldName string) ([]string, error) {
	// Load the data
	data, err := loadDataCsv(csvPath)
	if err != nil {
		return nil, err
	}

	// Map the field name to an index.
	var fieldIndex = -1
	for i, field := range data[0] {
		if fieldName == field {
			fieldIndex = i
		}
	}

	log.Infof("%v", data[0])

	if fieldIndex < 0 {
		log.Errorf("Could not find field %s in data", fieldName)
		return nil, fmt.Errorf("Could not find field %s in data", fieldName)
	}

	// Get the distinct category values.
	categories := make(map[string]bool)
	for _, row := range data[1:] {
		if !categories[row[fieldIndex]] {
			categories[row[fieldIndex]] = true
		}
	}

	// Extract the keys to return the possible categories.
	i := 0
	keys := make([]string, len(categories))
	for k := range categories {
		keys[i] = k
		i++
	}

	return keys, nil
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

func (s *Server) validateSession(sessionID string) *StatusErr {
	// If the session ID doesn't exist return a single result flagging the error
	// and close the stream.
	if !s.sessionIDs.Has(sessionID) {
		return &StatusErr{
			Status:   StatusCode_SESSION_UNKNOWN,
			ErrorMsg: fmt.Sprintf("session %s does not exist", sessionID),
		}
	}
	if s.endSessionIDs.Has(sessionID) {
		return &StatusErr{
			Status:   StatusCode_SESSION_EXPIRED,
			ErrorMsg: fmt.Sprintf("session %s already ended", sessionID),
		}
	}
	return nil
}
