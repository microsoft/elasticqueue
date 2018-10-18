package elasticqueue

import (
	"context"
	"time"

	"github.com/olivere/elastic"
)

// Requester is a simple interface around the client primarily
// for easy mocking in tests.
type Requester interface {
	// Send submits the bulk request to the server.
	Send(data []elastic.BulkableRequest) error
}

// ClientRequester is the default Requester that wraps an ElasticSearch client.
type ClientRequester struct {
	Client  *elastic.Client
	Timeout time.Duration
}

var _ Requester = &ClientRequester{}

// Send implements Requester.Send.
func (c *ClientRequester) Send(data []elastic.BulkableRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	_, err := c.Client.Bulk().Add(data...).Do(ctx)
	cancel()
	return err
}
