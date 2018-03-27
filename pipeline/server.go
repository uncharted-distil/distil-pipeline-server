package pipeline

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	// "math/rand"
	// "path/filepath"
	// "strconv"
	// "strings"
	// "sync"
	"golang.org/x/net/context"
	"time"
	// uuid generation
	"github.com/satori/go.uuid"
	// data structures
	"github.com/fatih/set"
	// grpc and protobuf
	"github.com/golang/protobuf/proto"
	protobuf "github.com/golang/protobuf/protoc-gen-go/descriptor"
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
	sessionIDs    *set.Set
	endSessionIDs *set.Set
	searchIDs     *set.Set
	pipelineIDs   *set.Set
	userAgent     string
	resultDir     string
	sendDelay     time.Duration
	numUpdates    int
	errPercentage float64
}

// NewServer creates a new pipeline server instance.  ID maps are initialized with place holder values
// to support tests without explicit calls to session management.
func NewServer(userAgent string, resultDir string, sendDelay int, numUpdates int, errPercentage float64) *Server {
	server := new(Server)
	server.sessionIDs = set.New("test-session-id")
	server.endSessionIDs = set.New("test-end-session-id")
	server.searchIDs = set.New()
	server.pipelineIDs = set.New("test-pipeline-id")
	server.userAgent = userAgent
	server.resultDir = resultDir
	server.sendDelay = time.Duration(sendDelay) * time.Millisecond
	server.numUpdates = numUpdates
	server.errPercentage = errPercentage
	return server
}

// SearchPipelines generates a searchID, kicks off a pipeline search internally, and returns a SearchResponse immediately
func (s *Server) SearchPipelines(ctx context.Context, req *SearchPipelinesRequest) (*SearchPipelinesResponse, error) {
	log.Infof("Received SearchPipelines - %v", req)

	// generate search_id
	id := uuid.NewV4().String()
	s.searchIDs.Add(id)

	resp := &SearchPipelinesResponse{SearchId: id}
	go s.startSearch(req)
	return resp, nil
}

// startSearch generates pipeline search results to be available when called in GetSearchPipelinesResults
func (s *Server) startSearch(req *SearchPipelinesRequest) {
}

func (s *Server) GetSearchPipelinesResults(req *GetSearchPipelinesResultsRequest, stream Core_GetSearchPipelinesResultsServer) error {
	return status.Error(codes.Unimplemented, "Method unimplemented")
}

func (s *Server) EndSearchPipelines(ctx context.Context, req *EndSearchPipelinesRequest) (*EndSearchPipelinesResponse, error) {
	searchID := req.GetSearchId()
	err := s.releaseSearchResources(searchID)
	if err != nil {
		// handle the err bruv
		// generate an appropriate grpc error code
		// err = status.Error(<code>, <msg>)
	}
	return &EndSearchPipelinesResponse{}, err
}

// releaseSearchResources releases all system resources related to searchID
func (s *Server) releaseSearchResources(searchID string) error {
	return nil
}

func (s *Server) StopSearchPipelines(ctx context.Context, req *StopSearchPipelinesRequest) (*StopSearchPipelinesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

func (s *Server) DescribePipeline(ctx context.Context, req *DescribePipelineRequest) (*DescribePipelineResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

func (s *Server) ScorePipeline(ctx context.Context, req *ScorePipelineRequest) (*ScorePipelineResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

func (s *Server) GetScorePipelineResults(req *GetScorePipelineResultsRequest, stream Core_GetScorePipelineResultsServer) error {
	return status.Error(codes.Unimplemented, "Method unimplemented")
}

func (s *Server) FitPipeline(ctx context.Context, req *FitPipelineRequest) (*FitPipelineResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

func (s *Server) GetFitPipelineResults(req *GetFitPipelineResultsRequest, stream Core_GetFitPipelineResultsServer) error {
	return status.Error(codes.Unimplemented, "Method unimplemented")
}

func (s *Server) ProducePipeline(ctx context.Context, req *ProducePipelineRequest) (*ProducePipelineResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

func (s *Server) GetProducePipelineResults(req *GetProducePipelineResultsRequest, stream Core_GetProducePipelineResultsServer) error {
	return status.Error(codes.Unimplemented, "Method unimplemented")
}

func (s *Server) PipelineExport(ctx context.Context, req *PipelineExportRequest) (*PipelineExportResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

func (s *Server) ListPrimitives(ctx context.Context, req *ListPrimitivesRequest) (*ListPrimitivesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// TODO(jtorrez): implement this if it stays in MR, may not be in final API
func (s *Server) StartSession(ctx context.Context, req *StartSessionRequest) (*StartSessionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// TODO(jtorrez): implement this if it stays in MR, may not be in final API
func (s *Server) EndSession(ctx context.Context, req *EndSessionRequest) (*EndSessionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// TODO(jtorrez): implement this if it stays in MR, may not be in final API
func (s *Server) StartProblem(ctx context.Context, req *StartProblemRequest) (*StartProblemResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

func (s *Server) UpdateProblem(ctx context.Context, req *UpdateProblemRequest) (*UpdateProblemResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

// TODO(jtorrez): implement this if it stays in MR, may not be in final API
func (s *Server) EndProblem(ctx context.Context, req *EndProblemRequest) (*EndProblemResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Method unimplemented")
}

func buildLookup(d3mIndexCol int, csvPath string, fieldName string) (map[string]string, error) {
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
	lookup := make(map[string]string)
	for _, row := range data[1:] {
		lookup[row[d3mIndexCol]] = row[fieldIndex]
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
	log.Infof("Categories: %v", keys)

	return keys, nil
}

// func (s *Server) validateSession(sessionID string) *StatusErr {
// 	// If the session ID doesn't exist return a single result flagging the error
// 	// and close the stream.
// 	if !s.sessionIDs.Has(sessionID) {
// 		return &StatusErr{
// 			Status:   StatusCode_SESSION_UNKNOWN,
// 			ErrorMsg: fmt.Sprintf("session %s does not exist", sessionID),
// 		}
// 	}
// 	if s.endSessionIDs.Has(sessionID) {
// 		return &StatusErr{
// 			Status:   StatusCode_SESSION_EXPIRED,
// 			ErrorMsg: fmt.Sprintf("session %s already ended", sessionID),
// 		}
// 	}
// 	return nil
// }
