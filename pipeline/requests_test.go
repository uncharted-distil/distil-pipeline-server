package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testMsg struct {
	IntA int
	IntB int
}

// Tests adding to an emtpy request set
func TestEmptyAddRequest(t *testing.T) {
	msg := &testMsg{100, 200}
	serverRequests := NewServerRequests()
	testRequest, err := serverRequests.AddRequest(rootKey, msg)
	assert.NoError(t, err)
	assert.Empty(t, testRequest.GetChildren())
	assert.Equal(t, rootKey, testRequest.GetParent())
	assert.Equal(t, msg, testRequest.GetRequestMsg())
	assert.NotEmpty(t, testRequest.GetRequestID())

	request, err := serverRequests.GetRequest(testRequest.GetRequestID())
	assert.NoError(t, err)
	assert.Exactly(t, request, testRequest)
}

// Tests adding children to an existing request
func TestAddChildRequest(t *testing.T) {
	msg := &testMsg{100, 200}
	serverRequests := NewServerRequests()
	testRequest, err := serverRequests.AddRequest(rootKey, msg)

	childMsg1 := &testMsg{300, 400}
	childRequest1, err := serverRequests.AddRequest(testRequest.GetRequestID(), childMsg1)
	assert.NoError(t, err)
	assert.Empty(t, childRequest1.GetChildren())
	assert.Equal(t, testRequest.GetRequestID(), childRequest1.GetParent())

	_, err = serverRequests.GetRequest(testRequest.GetRequestID())
	assert.NoError(t, err)

	childMsg2 := &testMsg{500, 600}
	childRequest2, err := serverRequests.AddRequest(testRequest.GetRequestID(), childMsg2)

	assert.Contains(t, testRequest.GetChildren(), childRequest1.GetRequestID())
	assert.Contains(t, testRequest.GetChildren(), childRequest2.GetRequestID())
}

// Tests removing several generations of child requests for a single root request
func TestRemoveRequest(t *testing.T) {
	msg := &testMsg{100, 200}
	serverRequests := NewServerRequests()
	testRequest, err := serverRequests.AddRequest(rootKey, msg)
	assert.NoError(t, err)

	childMsg1 := &testMsg{300, 400}
	childRequest1, err := serverRequests.AddRequest(testRequest.GetRequestID(), childMsg1)
	assert.NoError(t, err)

	childMsg2 := &testMsg{300, 400}
	childRequest2, err := serverRequests.AddRequest(testRequest.GetRequestID(), childMsg2)
	assert.NoError(t, err)

	grandChildMsg1 := &testMsg{500, 600}
	_, err = serverRequests.AddRequest(childRequest1.GetRequestID(), grandChildMsg1)
	assert.NoError(t, err)

	grandChildMsg2 := &testMsg{700, 800}
	_, err = serverRequests.AddRequest(childRequest2.GetRequestID(), grandChildMsg2)
	assert.NoError(t, err)

	err = serverRequests.RemoveRequest(testRequest.GetRequestID())
	assert.NoError(t, err)
	assert.Equal(t, len(serverRequests.nodes), 1)
	assert.Contains(t, serverRequests.nodes, rootKey)
}

func TestCompleteness(t *testing.T) {
	msg := &testMsg{100, 200}
	serverRequests := NewServerRequests()
	testRequest, err := serverRequests.AddRequest(rootKey, msg)
	assert.NoError(t, err)

	serverRequests.SetComplete(testRequest.GetRequestID())
	assert.True(t, serverRequests.IsComplete(testRequest.GetRequestID()))
}
