package pipeline

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"math/rand"
	// "path/filepath"
	// "strconv"
	// "strings"
	"golang.org/x/net/context"
	"sync"
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
	endSearchIDs  *set.Set
	pipelineIDs   *set.Set
	userAgent     string
	resultDir     string
	sendDelay     time.Duration
	numUpdates    int
	errPercentage float64
	maxPipelines  int
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
	server.pipelineIDs = set.New("test-pipeline-id")
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

	// NOTE(jtorrez): could get fancy here and kick-off a goroutine that starts generating pipeline results
	// but leaving that out of first pass dummy results implementation, should also be analyzing request to
	// for problem, problem_id (if that is kept in the API), template, etc.

	resp := &SearchPipelinesResponse{SearchId: id}
	return resp, nil
}

func (s *Server) GetSearchPipelinesResults(req *GetSearchPipelinesResultsRequest, stream Core_GetSearchPipelinesResultsServer) error {
	searchID := req.GetSearchId()
	err := s.validateSearch(searchID)
	if err != nil {
		return err
	}

	// randomly generate number of pipelines to "find"
	pipelinesFound := rand.Intn(s.maxPipelines)

	// if no pipelines are found, return immedidately
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
			// save the pipeline ID for subsequent calls
			s.pipelineIDs.Add(pipelineID)
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
			// send pipeline
			time.Sleep(s.sendDelay)
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

func (s *Server) EndSearchPipelines(ctx context.Context, req *EndSearchPipelinesRequest) (*EndSearchPipelinesResponse, error) {
	searchID := req.GetSearchId()
	err := s.validateSearch(searchID)
	if err != nil {
		return nil, err
	}

	s.endSearchIDs.Add(searchID)
	s.searchIDs.Remove(searchID)
	return &EndSearchPipelinesResponse{}, nil
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
