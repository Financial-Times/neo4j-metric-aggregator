package concept

import (
	"fmt"
	"time"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

const countAnnotationsQuery = `
	MATCH (canonicalConcept :Concept{prefUUID:{uuid}})<-[:EQUIVALENT_TO]-(x:Concept)
	OPTIONAL MATCH (x)-[]-(content:Content)
	WITH canonicalConcept, count(content) AS totalCount, COLLECT(DISTINCT(content)) as contentList
    UNWIND
		CASE
		  WHEN contentList = []
			 THEN [null]
		  ELSE contentList
		END AS cl
	MATCH (cl)
	WHERE cl.publishedDateEpoch > {since} OR cl IS null 
	RETURN canonicalConcept.prefUUID, count(cl) AS recentCount, totalCount
`

type AnnotationsCounter interface {
	Count(conceptUUIDs []string) (map[string]Metrics, error)
}

func NewAnnotationsCounter(driverPool bolt.DriverPool) AnnotationsCounter {
	return &neoAnnotationsCounter{driverPool}
}

type neoAnnotationsCounter struct {
	driverPool bolt.DriverPool
}

func (c *neoAnnotationsCounter) Count(conceptUUIDs []string) (map[string]Metrics, error) {
	conn, err := c.driverPool.OpenPool()
	if err != nil {
		return nil, fmt.Errorf("error in creating a connection to Neo4j: %w", err)
	}
	defer conn.Close()

	queries, parameterSets := buildAnnotationsCountPipelineComponents(conceptUUIDs)
	rows, err := conn.QueryPipeline(queries, parameterSets...)
	if err != nil {
		return nil, fmt.Errorf("error in executing query pipeline in Neo4j: %w", err)
	}
	retval := make(map[string]Metrics)

	var row []interface{}
	var nextPipelineRows bolt.PipelineRows

	for rows != nil {
		row, _, nextPipelineRows, err = rows.NextPipeline()
		if err != nil {
			return nil, fmt.Errorf("error in parsing query reults: %w", err)
		}
		if row == nil {
			rows = nextPipelineRows
			continue
		}
		conceptUUID, ok := row[0].(string)
		if ok {
			prevWeekAnnotationsCount, okWeekCount := row[1].(int64)
			totalCount, okTotalCount := row[2].(int64)
			if !okWeekCount || !okTotalCount {
				return nil, fmt.Errorf("unexpected count type: prevWeekAnnotationsCount is %T, totalCount is %T", prevWeekAnnotationsCount, totalCount)
			}
			retval[conceptUUID] = Metrics{PrevWeekAnnotationsCount: prevWeekAnnotationsCount, AnnotationsCount: totalCount}
		}
	}

	return retval, nil
}

func buildAnnotationsCountPipelineComponents(conceptUUIDs []string) ([]string, []map[string]interface{}) {
	var queries []string
	var parameterSets []map[string]interface{}

	now := time.Now().Unix()
	weekAgo := now - 7*24*3600
	for _, uuid := range conceptUUIDs {
		queries = append(queries, countAnnotationsQuery)
		params := map[string]interface{}{"uuid": uuid, "since": weekAgo}
		parameterSets = append(parameterSets, params)
	}
	return queries, parameterSets
}
