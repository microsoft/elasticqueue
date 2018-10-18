package elasticqueue

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/olivere/elastic"
)

func defaultErrorHandler(err error) {
	log.Printf("mixer/elasticqueue: error writing to elasticsearch: %s. "+
		"(note: pass WithErrorHandler() to override default logging)", err)
}

// NewQueue creates a new ElasticSearch queue around the provided client.
// Note that you are required to provide at least on Condition using
// WithCondition.
func NewQueue(client *elastic.Client, options ...Option) *Queue {
	queue := &Queue{
		errorHandler: defaultErrorHandler,
		closer:       make(chan struct{}),
	}

	for _, option := range options {
		option(queue)
	}

	if queue.requester == nil {
		queue.requester = &ClientRequester{Client: client, Timeout: queue.timeout}
	}

	if len(queue.conditions) == 0 {
		panic("mixer/elasticqueue: write conditions were passed, the client will buffer " +
			"infinitely! Use WithCondition() to pass one or more options to NewQueue()")
	}

	go queue.listenToConditions()

	return queue
}

// Queue is the implementation of the ElasticSearch queue.
type Queue struct {
	requester   Requester
	conditions  []Condition
	writeWaiter sync.WaitGroup
	closer      chan struct{}

	timeout      time.Duration
	backoff      elastic.Backoff
	errorHandler func(err error)

	queueMu sync.Mutex
	queue   []elastic.BulkableRequest
}

// Store writes the document, or queues it for writing later. This is thread-safe.
func (q *Queue) Store(index, kind string, doc interface{}) (err error) {
	uuid, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("mixer/elasticqueue: error creating queue item UUID: %s", err)
	}

	return q.StoreWithId(index, kind, uuid.String(), doc)
}

// Store writes the document, or queues it for writing later. This is thread-safe.
func (q *Queue) StoreWithId(index, kind, id string, doc interface{}) (err error) {
	encoded, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("mixer/elasticqueue: error JSON encoding item queue of type %T: %s", doc, err)
	}

	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	q.queue = append(q.queue, elastic.NewBulkIndexRequest().
		Index(index).
		Type(kind).
		Id(id). // assign an ID so if we retry bulk ops, we don't double-write
		Doc(json.RawMessage(encoded)))

	writeNow := false
	for _, condition := range q.conditions {
		if condition.Inserted(encoded, len(q.queue)) {
			writeNow = true
		}
	}

	if writeNow {
		q.runBackgroundWrite()
	}

	return nil
}

// Close tears down resources and writes any pending operations.
func (q *Queue) Close() {
	q.closer <- struct{}{}
	<-q.closer // wait for the close to be confirmed
	q.writeWaiter.Wait()

	if len(q.queue) > 0 {
		q.writeImmediately(q.queue)
		q.queue = nil
	}
}

func (q *Queue) listenToConditions() {
	channels := []<-chan struct{}{q.closer}
	closers := []func(){func() { close(q.closer) }}

	for _, condition := range q.conditions {
		channel, closer := condition.Write()
		if channel == nil {
			continue
		}

		channels = append(channels, channel)
		closers = append(closers, closer)
	}

	cases := make([]reflect.SelectCase, 0, len(channels))
	for _, ch := range channels {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
	}

	for {
		chosen, _, _ := reflect.Select(cases)
		if chosen == 0 { // closer
			break
		}

		q.queueMu.Lock()
		q.runBackgroundWrite()
		q.queueMu.Unlock()
	}

	for _, closer := range closers {
		closer()
	}
}

func (q *Queue) writeImmediately(queue []elastic.BulkableRequest) {
	err := elastic.Retry(
		func() error { return q.requester.Send(queue) },
		q.backoff,
	)

	if err != nil {
		q.errorHandler(err)
	}
}

func (q *Queue) runBackgroundWrite() {
	queue := q.queue
	q.queue = nil

	if len(queue) == 0 {
		return
	}

	for _, condition := range q.conditions {
		condition.Flushed()
	}

	q.writeWaiter.Add(1)

	go func() {
		q.writeImmediately(queue)
		q.writeWaiter.Done()
	}()
}
