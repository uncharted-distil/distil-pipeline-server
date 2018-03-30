package pipeline

import (
	"sync"

	"github.com/fatih/set"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

// RequestNode stores data for a hierarchical server API request.
type RequestNode interface {
	GetRequestID() string
	GetParent() string
	GetChildren() []string
	GetRequestMsg() interface{}
}

type baseRequestNode struct {
	requestID  string
	parent     string
	children   []string
	requestMsg interface{}
	lock       *sync.RWMutex
}

func (s *baseRequestNode) GetRequestID() string {
	return s.requestID
}

func (s *baseRequestNode) GetParent() string {
	return s.parent
}

func (s *baseRequestNode) GetChildren() []string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return append([]string(nil), s.children...)
}

func (s *baseRequestNode) GetRequestMsg() interface{} {
	return s.requestMsg
}

// ServerRequests provides thread safe access to API request information
type ServerRequests struct {
	searches []string
	nodes    map[string]*baseRequestNode
	complete *set.Set
	lock     *sync.RWMutex
}

// NewServerRequests creates a new instance of a server request manager
func NewServerRequests() *ServerRequests {
	return &ServerRequests{
		searches: []string{},
		nodes:    map[string]*baseRequestNode{},
		complete: set.New(),
		lock:     new(sync.RWMutex),
	}
}

// AddRequest adds a new server API request
func (s *ServerRequests) AddRequest(parentID string, requestMsg interface{}) (RequestNode, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	parentNode, ok := s.nodes[parentID]
	if !ok {
		return nil, errors.Errorf("parent node %s not found", parentID)
	}
	requestNode := &baseRequestNode{
		requestID:  uuid.NewV4().String(),
		parent:     parentID,
		children:   []string{},
		requestMsg: requestMsg,
		lock:       s.lock,
	}
	parentNode.children = append(parentNode.children, requestNode.requestID)
	s.nodes[requestNode.requestID] = requestNode
	return requestNode, nil
}

// RemoveRequest request removes a request node and all of its descendants.
func (s *ServerRequests) RemoveRequest(requestID string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.doRemove(requestID) {
		return errors.Errorf("failed to remove request %s or one of its descendants", requestID)
	}
	return nil
}

func (s *ServerRequests) doRemove(requestID string) bool {
	node, ok := s.nodes[requestID]
	if !ok {
		return false
	}
	for _, child := range node.children {
		ok = s.doRemove(child)
		if !ok {
			return false
		}
	}
	return true
}

// GetRequest returns a previously added server API request
func (s *ServerRequests) GetRequest(requestID string) (RequestNode, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	node, ok := s.nodes[requestID]
	if !ok {
		return nil, errors.Errorf("node %s not found", requestID)
	}
	return node, nil
}

// SetComplete marks a server API request as having completed processing
func (s *ServerRequests) SetComplete(requestID string) {
	s.complete.Add(requestID)
}

// IsComplete indicates whether or not a server request is in a complete state.
func (s *ServerRequests) IsComplete(requestID string) bool {
	return s.complete.Has(requestID)
}
