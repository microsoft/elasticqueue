package elasticqueue

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ElasticQueueSuite struct {
	suite.Suite
	r *mockRequester
	c *mockCondition
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

	err := m.err
	m.err = nil
	return err
}

var _ Requester = &mockRequester{}

type mockCondition struct {
	mock.Mock
	writeChannel chan struct{}
}

func (m *mockCondition) Inserted(document []byte, length int) bool {
	return m.Called(json.RawMessage(document), length).Bool(0)
}

func (m *mockCondition) Write() (doWrite <-chan struct{}, cancel func()) {
	return m.writeChannel, func() { close(m.writeChannel) }
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

func (e *ElasticQueueSuite) TestPanicsIfCreatedWithoutCounditions() {
	e.Panics(func() { NewQueue(nil, WithRequester(e.r)) })
}

func (e *ElasticQueueSuite) TestRunsWriteOnInsertedCheck() {
	q := NewQueue(nil, WithRequester(e.r), WithCondition(e.c))
	e.c.On("Inserted", mockDocument1, 1).Return(true)
	e.c.On("Flushed").Return()
	e.Nil(q.Store("foo", "bar", mockDocument1))

	e.Len(e.r.WaitForData(1), 1)
	e.Equal(1, e.r.calls)
	q.Close()

	e.Equal(1, e.r.calls)
}

func (e *ElasticQueueSuite) TestFlushesOutstandingDataOnClose() {
	q := NewQueue(nil, WithRequester(e.r), WithCondition(e.c))
	e.c.On("Inserted", mockDocument1, 1).Return(false)
	e.Nil(q.Store("foo", "bar", mockDocument1))

	e.Len(e.r.WaitForData(1), 0)

	e.Equal(0, e.r.calls)
	q.Close()
	e.Equal(1, e.r.calls)
	e.Len(e.r.data, 1)
}

func (e *ElasticQueueSuite) TestHandlesError() {
	err := errors.New("oh no")
	gotError := false
	e.r.err = err

	q := NewQueue(nil, WithRequester(e.r), WithCondition(e.c), WithErrorHandler(func(actualErr error) {
		gotError = true
		e.Equal(err, actualErr)
	}))

	e.c.On("Inserted", mockDocument1, 1).Return(false)
	e.Nil(q.Store("foo", "bar", mockDocument1))
	q.Close()

	e.True(gotError)
}

func (e *ElasticQueueSuite) TestRunsBackoff() {
	e.r.err = errors.New("oh no")
	q := NewQueue(nil,
		WithRequester(e.r),
		WithCondition(e.c),
		WithBackoff(elastic.NewConstantBackoff(time.Millisecond*10)),
		WithErrorHandler(func(_ error) { e.Fail("expected not to get an error") }))

	e.c.On("Inserted", mockDocument1, 1).Return(false)
	e.Nil(q.Store("foo", "bar", mockDocument1))
	q.Close()

	e.Equal(2, e.r.calls)
}

func (e *ElasticQueueSuite) TestWritesOnBackgroundChannel() {
	e.c.writeChannel = make(chan struct{})

	q := NewQueue(nil,
		WithRequester(e.r),
		WithCondition(e.c))

	e.c.On("Inserted", mockDocument1, 1).Return(false)
	e.c.On("Flushed").Return()
	e.Nil(q.Store("foo", "bar", mockDocument1))

	e.c.writeChannel <- struct{}{}
	e.Len(e.r.WaitForData(1), 1)
	e.Equal(1, e.r.calls)

	q.Close()

	e.Equal(1, e.r.calls)
	<-e.c.writeChannel // expect to have closed the channel
}
