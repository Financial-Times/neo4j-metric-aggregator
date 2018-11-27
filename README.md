# neo4j-metric-aggregator

[![Circle CI](https://circleci.com/gh/Financial-Times/neo4j-metric-aggregator/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/neo4j-metric-aggregator/tree/master) [![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/neo4j-metric-aggregator)](https://goreportcard.com/report/github.com/Financial-Times/neo4j-metric-aggregator) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/neo4j-metric-aggregator/badge.svg)](https://coveralls.io/github/Financial-Times/neo4j-metric-aggregator)

## Introduction

A microservice to compute metrics on neo4j knowledge base.

## Installation      

Download the source code, dependencies and test dependencies:

        curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
        go get -u github.com/Financial-Times/neo4j-metric-aggregator
        cd $GOPATH/src/github.com/Financial-Times/neo4j-metric-aggregator
        dep ensure
        go build .

## Running locally

1. Run Neo4J

	For running integration tests:

```
	docker run \
		--publish=7474:7474 --publish=7687:7687 \
		-e NEO4J_ACCEPT_LICENSE_AGREEMENT="yes" \
		-e NEO4J_AUTH="none" \
		neo4j:3.2.7-enterprise
```

2. Run the tests and install the binary:

    * Fetch all dependencies: `dep ensure`
    * Run tests
        * Unit tests only: `go test -race ./...`
        * Unit and integration tests: `go test -race -tags=integration ./...`
    * Install the binary: `go install`

3. Run the binary (using the `help` flag to see the available optional arguments):

        $GOPATH/bin/neo4j-metric-aggregator [--help]

        Options:
                     
            --app-system-code         		System Code of the application (env $APP_SYSTEM_CODE) (default "neo4j-metric-aggregator")
            --app-name                		Application name (env $APP_NAME) (default "neo4j-metric-aggregator")
            --port                    		Port to listen on (env $PORT) (default "8080")
            --neo4j-endpoint          		URL of the Neo4j bolt endpoint (env $NEO4J_ENDPOINT) (default "bolt://localhost:7687")
            --neo4j-max-connections   		The maximum number of parallel connections to Neo4J (env $NEO4J_MAX_CONNECTIONS) (default 10)
            --maxRequestBatchSize     		The maximum number of concepts per request (env $MAX_REQUEST_BATCH_SIZE) (default 20)
			--recentAnnotationsCountAge		max age of counted annotations in recentAnnotations in seconds (env #RECENT_ANNOTATIONS_COUNT_AGE) (default 604800)


## Build and deployment

* Built by Jenkins when a tag is created and pushed the docker image to Docker Hub: [coco/neo4j-metric-aggregator](https://hub.docker.com/r/coco/neo4j-metric-aggregator/)
* CI provided by CircleCI: [neo4j-metric-aggregator](https://circleci.com/gh/Financial-Times/neo4j-metric-aggregator)

## Service endpoints

### Get metrics for concepts

Using curl:

    curl http://localhost:8080/concepts/metrics?uuids=<uuid1>,<uuid2,<uuid3>,...<uuidN> | json_pp`

The response payload contains metrics about a concepts in Neo4j knowledge base. 
An example is provided below:

```json
[
    {
        "uuid": "d6b12f0c-bf3f-4045-a07b-1e4e49103fd1",
        "metrics": {
			"annotationsCount": {
				"recent": 2,
				"total": 125
			}
        }
    },
    {
        "uuid": "a4de0e8f-96f4-4ccf-ba26-410f005e021b",
        "metrics": {
            "annotationsCount": {
				"recent": 0,
				"total": 1250
			}
        }
    },    
    {
        "uuid": "e5115380-59db-41cf-9356-672f73d6208f",
        "metrics": {
			"annotationsCount": {
				"recent": 0,
				"total": 0
			}
        }
    }
]
``` 

## Utility endpoints
_Endpoints that are there for support or testing, e.g read endpoints on the writers_

## Healthchecks
Admin endpoints are:

`/__gtg`

`/__health`

`/__build-info`

At the moment, the health endpoint checks that a connection can be made to Neo4j, 
using the neo4j url supplied as a parameter in service startup.

### Logging

* The application uses [logrus](https://github.com/sirupsen/logrus); the log file is initialised in [main.go](main.go).
* NOTE: `/__build-info` and `/__gtg` endpoints are not logged as they are called every second from varnish/vulcand and this information is not needed in logs/splunk.
