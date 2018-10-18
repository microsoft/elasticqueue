package elasticqueue

import (
	"time"

	"github.com/olivere/elastic"
)

// Option is passed to NewQueue to configure it.
type Option func(q *Queue)

// WithTimeout sets the timeout for ElasticSearch write operations.
func WithTimeout(timeout time.Duration) Option {
	return func(q *Queue) {
		q.timeout = timeout
	}
}

// WithBackoff sets the backoff for ElasticSearch write retries.
func WithBackoff(backoff elastic.Backoff) Option {
	return func(q *Queue) {
		q.backoff = backoff
	}
}

// WithErrorHandler sets the function that's called whenever an error
// occurs in a background ElasticSearch write.
func WithErrorHandler(errorHandler func(err error)) Option {
	return func(q *Queue) {
		q.errorHandler = errorHandler
	}
}

// WithCondition sets the write conditions for the queue.
func WithCondition(conditions ...Condition) Option {
	return func(q *Queue) {
		q.conditions = append(q.conditions, conditions...)
	}
}

// WithRequester sets the requester for the queue. This is primarily for
// testing purposes and can usually omitted unless you'd like low-level control
// over client behavior.
func WithRequester(requester Requester) Option {
	return func(q *Queue) {
		q.requester = requester
	}
}
