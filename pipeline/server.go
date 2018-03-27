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
	"time"

	"github.com/fatih/set"
	"github.com/golang/protobuf/proto"
	protobuf "github.com/golang/protobuf/protoc-gen-go/descriptor"
	// "github.com/satori/go.uuid"
	"github.com/unchartedsoftware/plog"
	// "golang.org/x/net/context"
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
	numUpdates    int
	errPercentage float64
}

// // StatusErr provides an status code and an error message
// type StatusErr struct {
// 	// Status   StatusCode
// 	ErrorMsg string
// }

// func (s *StatusErr) Error() string {
// 	return s.ErrorMsg
// }

// NewServer creates a new pipeline server instance.  ID maps are initialized with place holder values
// to support tests without explicit calls to session management.
func NewServer(userAgent string, resultDir string, sendDelay int, numUpdates int, errPercentage float64) *Server {
	server := new(Server)
	server.sessionIDs = set.New("test-session-id")
	server.endSessionIDs = set.New("test-end-session-id")
	server.pipelineIDs = set.New("test-pipeline-id")
	server.userAgent = userAgent
	server.resultDir = resultDir
	server.sendDelay = time.Duration(sendDelay) * time.Millisecond
	server.numUpdates = numUpdates
	server.errPercentage = errPercentage
	return server
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
