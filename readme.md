# elasticqueue

A utility library around [olivere/elastic](https://github.com/olivere/elastic) which lets you insert records to be written into a queue, which then gets bulked up when it meets a certain condition and sent to ElasticSearch.
