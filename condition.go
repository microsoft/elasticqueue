package elasticqueue

import (
	"math"
	"time"

	"github.com/mixer/clock"
)

// Condition is passed to the Queue to determine when it should be written out.
type Condition interface {
	// Inserted is called synchronously whenever a document is inserted into
	// the ElasticSearch queue. It should return true if the queue should be
	// immediately flushed to ElasticSearch.
	Inserted(document []byte, length int) (writeImmediately bool)

	// Write is called by the Queue once when first created. It returns a
	// channel which should emit whenever. This may return nil if the
	// condition does not care to do asynchronous writes.
	Write() (doWrite <-chan struct{}, cancel func())

	// Flushed is called whenever the queue is written out.
	Flushed()
}

var maxDuration = time.Duration(math.MaxInt64)

// writeAfterTimer is a helper function that returns the data wanted by
// Condition.Write after the timer fires.
func writeAfterTimer(timer clock.Timer) (doWrite <-chan struct{}, cancel func()) {
	ch := make(chan struct{})
	closer := make(chan struct{})
	go func() {
		for {
			select {
			case <-timer.Chan():
				select {
				case ch <- struct{}{}:
				case <-closer:
					return
				}
			case <-closer:
				return
			}
		}
	}()

	return ch, func() { close(ch); timer.Stop() }
}

// WriteAfterIdle returns a write condition which'll cause the queue to be
// written out to ElasticSearch after no documents are written for
// a period of time.
func WriteAfterIdle(interval time.Duration) Condition {
	return &afterIdleCondition{
		interval: interval,
		timer:    clock.C.NewTimer(maxDuration),
	}
}

var _ Condition = &afterIdleCondition{}

type afterIdleCondition struct {
	timer    clock.Timer
	interval time.Duration
}

// Inserted implements Condition.Inserted
func (a *afterIdleCondition) Inserted(_ []byte, _ int) (writeImmediately bool) {
	a.timer.Reset(a.interval)
	return false
}

// Write implements Condition.Write
func (a *afterIdleCondition) Write() (doWrite <-chan struct{}, cancel func()) {
	return writeAfterTimer(a.timer)
}

// Flushed implements Condition.Flushed
func (a *afterIdleCondition) Flushed() { a.timer.Reset(maxDuration) }

// WriteAfterInterval returns a write condition which'll cause the queue to be
// written out after a constant amount of time after the first write.
func WriteAfterInterval(interval time.Duration) Condition {
	return &afterIntervalCondition{timer: clock.C.NewTimer(maxDuration), interval: interval}
}

var _ Condition = &afterIntervalCondition{}

type afterIntervalCondition struct {
	timer    clock.Timer
	interval time.Duration
}

// Inserted implements Condition.Inserted
func (a *afterIntervalCondition) Inserted(_ []byte, length int) (writeImmediately bool) {
	if length == 1 {
		a.timer.Reset(a.interval)
	}

	return false
}

// Write implements Condition.Write
func (a *afterIntervalCondition) Write() (doWrite <-chan struct{}, cancel func()) {
	return writeAfterTimer(a.timer)
}

// Flushed implements Condition.Flushed
func (a *afterIntervalCondition) Flushed() { a.timer.Reset(maxDuration) }

// WriteAfterLength returns a write condition which'll cause the queue to be
// written out after it reaches a predefined length.
func WriteAfterLength(length int) Condition { return afterLengthCondition{length} }

var _ Condition = afterLengthCondition{}

type afterLengthCondition struct{ length int }

// Inserted implements Condition.Inserted
func (a afterLengthCondition) Inserted(_ []byte, length int) (writeImmediately bool) {
	return length == a.length
}

// Write implements Condition.Write
func (a afterLengthCondition) Write() (doWrite <-chan struct{}, cancel func()) { return }

// Flushed implements Condition.Flushed
func (a afterLengthCondition) Flushed() {}

// WriterAfterByteSize returns a write condition which'll cause the queue to be
// written out after it's more than "maxBytes" bytes long.
func WriterAfterByteSize(maxBytes int) Condition { return &afterByteSize{maxSize: maxBytes} }

var _ Condition = &afterByteSize{}

type afterByteSize struct {
	maxSize     int
	currentSize int
}

// Inserted implements Condition.Inserted
func (a *afterByteSize) Inserted(document []byte, _ int) (writeImmediately bool) {
	a.currentSize += len(document)
	return a.currentSize >= a.maxSize
}

// Write implements Condition.Write
func (a *afterByteSize) Write() (doWrite <-chan struct{}, cancel func()) { return }

// Flushed implements Condition.Flushed
func (a *afterByteSize) Flushed() { a.currentSize = 0 }
