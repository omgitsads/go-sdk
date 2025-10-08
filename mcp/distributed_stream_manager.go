package mcp

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

type RequestStream interface {
	StreamID() string
	isInitialize() bool
	JSONResponse() bool // corrected from boolbool

	Lock() bool
	Unlock() bool

	AwaitMessages(ctx context.Context) *chan struct{}
	SignalNewMessages()
}

type InMemoryRequestStream struct {
	// id is the logical ID for the stream, unique within a session.
	// an empty string is used for messages that don't correlate with an incoming request.
	id string

	// If isInitialize is set, the stream is in response to an initialize request,
	// and therefore should include the session ID header.
	isInitializeStream bool

	// jsonResponse records whether this stream should respond with application/json
	// instead of text/event-stream.
	//
	// See [StreamableServerTransportOptions.JSONResponse].
	isJSONResponse bool

	// signal is a 1-buffered channel, owned by an incoming HTTP request, that signals
	// that there are messages available to write into the HTTP response.
	// In addition, the presence of a channel guarantees that at most one HTTP response
	// can receive messages for a logical stream. After claiming the stream, incoming
	// requests should read from the event store, to ensure that no new messages are missed.
	//
	// To simplify locking, signal is an atomic. We need an atomic.Pointer, because
	// you can't set an atomic.Value to nil.
	//
	// Lifecycle: each channel value persists for the duration of an HTTP POST or
	// GET request for the given streamID.
	signal atomic.Pointer[chan struct{}]

	// The following mutable fields are protected by the mutex of the containing
	// StreamableServerTransport.

	// streamRequests is the set of unanswered incoming RPCs for the stream.
	//
	// Requests persist until their response data has been added to the event store.
	requests map[jsonrpc.ID]struct{}
}

func (s *InMemoryRequestStream) StreamID() string {
	return s.id
}

func (s *InMemoryRequestStream) isInitialize() bool {
	return s.isInitializeStream
}

func (s *InMemoryRequestStream) JSONResponse() bool {
	return s.isJSONResponse
}

func (s *InMemoryRequestStream) Lock() bool {
	ch := make(chan struct{}, 1)
	return s.signal.CompareAndSwap(nil, &ch)
}

func (s *InMemoryRequestStream) Unlock() bool {
	return s.signal.CompareAndSwap(s.signal.Load(), nil)
}

func (s *InMemoryRequestStream) AwaitMessages(ctx context.Context) *chan struct{} {
	return s.signal.Load()
}

func (s *InMemoryRequestStream) SignalNewMessages() {
	chPtr := s.signal.Load()
	if chPtr != nil {
		select {
		case *chPtr <- struct{}{}:
		default:
			// channel already has a message, so the receiver is already awake
		}
	}
}

type StreamManager interface {
	// SessionID for related streams
	SessionID() string

	// NewStream creates a new RequestStream for a given stream ID.
	NewStream(ctx context.Context, streamID string, isInitialize bool, jsonResponse bool) (RequestStream, error)

	// SetRequestStream associates a request ID with a stream ID.
	SetRequestStream(ctx context.Context, requestID jsonrpc.ID, stream RequestStream) error

	// GetRequestStream retrieves the stream ID associated with a given request ID.
	GetRequestStream(ctx context.Context, requestID jsonrpc.ID) (stream RequestStream, exists bool, err error)

	// RemoveRequestStream removes the association for a given request ID.
	// This should be called when a request is completed or cancelled.
	RemoveRequestStream(ctx context.Context, requestID jsonrpc.ID) error

	// NumberOfRequestsForStream returns the number of pending requests that have yet to be handled for a stream.
	NumberOfRequestsForStream(ctx context.Context, streamID string) int

	// Get a stream
	GetStream(ctx context.Context, streamID string) RequestStream
}

// InMemoryDistributedStreamManager provides an in-memory implementation of
// DistributedStreamManager. This is suitable for testing or single-instance
// scenarios, but does not provide true distributed capabilities.
type InMemoryDistributedStreamManager struct {
	mu             sync.RWMutex
	sessionID      string
	requestStreams map[jsonrpc.ID]RequestStream
	streams        map[string]RequestStream
}

// NewInMemoryDistributedStreamManager creates a new in-memory stream manager.
func NewInMemoryDistributedStreamManager(sessionID string) *InMemoryDistributedStreamManager {
	return &InMemoryDistributedStreamManager{
		sessionID: sessionID,
		streams:   make(map[string]RequestStream), // initializing streams map
	}
}

func (m *InMemoryDistributedStreamManager) SessionID() string {
	return m.sessionID
}

func (m *InMemoryDistributedStreamManager) NewStream(ctx context.Context, streamID string, isInitialize bool, jsonResponse bool) (RequestStream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if the stream already exists
	if existingStream, exists := m.streams[streamID]; exists {
		return existingStream, nil // return existing stream if found
	}

	// Create a new InMemoryRequestStream
	newStream := &InMemoryRequestStream{
		id:                 streamID,
		isInitializeStream: isInitialize,
		isJSONResponse:     jsonResponse,
		requests:           make(map[jsonrpc.ID]struct{}),
	}
	m.streams[streamID] = newStream // store the new stream in the streams map

	return newStream, nil
}

func (m *InMemoryDistributedStreamManager) SetRequestStream(ctx context.Context, requestID jsonrpc.ID, stream RequestStream) error {
	m.requestStreams[requestID] = stream
	m.streams[stream.StreamID()] = stream // adding stream to streams map
	return nil
}

func (m *InMemoryDistributedStreamManager) GetRequestStream(ctx context.Context, requestID jsonrpc.ID) (RequestStream, bool, error) {
	s, ok := m.requestStreams[requestID]
	if !ok {
		return nil, false, nil // return nil for stream and false for exists if not found
	}
	return s, true, nil // return the found stream and true for exists
}

func (m *InMemoryDistributedStreamManager) RemoveRequestStream(ctx context.Context, requestID jsonrpc.ID) error {
	delete(m.requestStreams, requestID)
	return nil
}

func (m *InMemoryDistributedStreamManager) NumberOfRequestsForStream(ctx context.Context, streamID string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stream, exists := m.streams[streamID]
	if !exists {
		return 0
	}
	return len(stream.(*InMemoryRequestStream).requests)
}

func (m *InMemoryDistributedStreamManager) GetStream(ctx context.Context, streamID string) RequestStream {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.streams[streamID]
}
