package elasticqueue

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/olivere/elastic"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ElasticQueueSuite struct {
	suite.Suite
	r     *mockRequester
	c     *mockCondition
	queue *Queue
}

var (
	mockDocument1 = json.RawMessage(`"hello world!"`)
)

type mockRequester struct {
	mu    sync.Mutex
	err   error
	data  []elastic.BulkableRequest
	calls int
}

func (m *mockRequester) WaitForData(length int) []elastic.BulkableRequest {
	deadline := time.Now().Add(time.Millisecond * 100)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		data := m.data
		if len(data) < length {
			m.mu.Unlock()
			time.Sleep(time.Millisecond * 2)
			continue
		}

		output := make([]elastic.BulkableRequest, len(data))
		copy(output, data)
		m.mu.Unlock()
		return data
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data
}

func (m *mockRequester) Send(data []elastic.BulkableRequest) error {
	m.mu.Lock()
	m.data = append(m.data, data...)
	m.calls++
	m.mu.Unlock()
	return m.err
}

var _ Requester = &mockRequester{}

type mockCondition struct {
	mock.Mock
	writeChannel chan struct{}
}

func (m *mockCondition) Inserted(document []byte, length int) bool {
	return m.Called([]byte(document), length).Bool(0)
}

func (m *mockCondition) Write() (doWrite <-chan struct{}, cancel func()) {
	return m.writeChannel, func() {}
}

func (m *mockCondition) Flushed() { m.Called() }

func TestElasticQueueSuite(t *testing.T) {
	suite.Run(t, new(ElasticQueueSuite))
}

func (e *ElasticQueueSuite) SetupTest() {
	e.r = &mockRequester{}
	e.c = &mockCondition{}
}

func (e *ElasticQueueSuite) TearDownTest() {
	e.c.AssertExpectations(e.T())
}

func (e *ElasticQueueSuite) TestRunsWriteOnInsertedCheck() {
	q := NewQueue(nil, WithRequester(e.r), WithCondition(e.c))
	e.c.On("Inserted", mockDocument1, 1).Return(true)
	e.Nil(q.Store("foo", "bar", mockDocument1))
	e.Len(e.r.WaitForData(1), 1)
	e.Equal(1, e.r.calls)
	e.queue.Close()

	e.Equal(1, e.r.calls)
}
