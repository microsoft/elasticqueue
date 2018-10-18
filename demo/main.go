package main

import (
	"time"

	"github.com/mixer/elasticqueue"
	"github.com/olivere/elastic"
)

type MyElasticSearchRecord struct {
	Cool bool
}

func main() {
	// Create your ElasticSearch client as you normally would.
	client, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}
	defer client.Stop()

	// Define a new queue. Here, our queue will send when either we don't
	// write any new documents for 10 seconds, or we have 100 documents waiting
	// to be submitted. We also define a backoff policy to automatically rewrite writes.
	queue := elasticqueue.NewQueue(client,
		elasticqueue.WithCondition(elasticqueue.WriteAfterIdle(10*time.Second)),
		elasticqueue.WithCondition(elasticqueue.WriteAfterLength(100)),
		elasticqueue.WithBackoff(elastic.NewExponentialBackoff(time.Second, time.Second*10)))

	// Make sure to gracefully close the queue before your application exits
	// so that any pending items get written out!
	defer queue.Close()

	// your logic...
	for i := 0; i < 10; i++ {
		queue.Store("my-index", "my-type", MyElasticSearchRecord{Cool: true})
	}
}
