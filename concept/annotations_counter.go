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
	Count(conceptUUIDs []string) (map[string]Stats, error)
}

func NewAnnotationsCounter(driverPool bolt.DriverPool, recentAnnotationsCountAge int) AnnotationsCounter {
	return &neoAnnotationsCounter{driverPool, recentAnnotationsCountAge}
}

type neoAnnotationsCounter struct {
	driverPool                bolt.DriverPool
	recentAnnotationsCountAge int
}

func (c *neoAnnotationsCounter) Count(conceptUUIDs []string) (map[string]Stats, error) {
	conn, err := c.driverPool.OpenPool()
	if err != nil {
		return nil, fmt.Errorf("error in creating a connection to Neo4j: %v", err.Error())
	}
	defer conn.Close()

	queries, parameterSets := buildAnnotationsCountPipelineComponents(conceptUUIDs, c.recentAnnotationsCountAge)
	rows, err := conn.QueryPipeline(queries, parameterSets...)
	if err != nil {
		return nil, fmt.Errorf("error in executing query pipeline in Neo4j: %v", err.Error())
	}
	retval := make(map[string]Stats)

	var row []interface{}
	var nextPipelineRows bolt.PipelineRows

	for rows != nil {
		row, _, nextPipelineRows, err = rows.NextPipeline()
		if err != nil {
			return nil, fmt.Errorf("error in parsing query reults: %v", err.Error())
		}
		if row == nil {
			rows = nextPipelineRows
			continue
		}
		conceptUUID, ok := row[0].(string)
		if ok {
			recentCount := row[1].(int64)
			totalCount := row[2].(int64)
			retval[conceptUUID] = Stats{recentCount, totalCount}
		}
	}

	return retval, nil
}

func buildAnnotationsCountPipelineComponents(conceptUUIDs []string, recentAnnotationsCountAge int) ([]string, []map[string]interface{}) {
	var queries []string
	var parameterSets []map[string]interface{}

	now := int64(time.Now().Unix())
	recentPeriodStart := now - int64(recentAnnotationsCountAge)
	for _, uuid := range conceptUUIDs {
		queries = append(queries, countAnnotationsQuery)
		params := map[string]interface{}{"uuid": uuid, "since": recentPeriodStart}
		parameterSets = append(parameterSets, params)
	}
	return queries, parameterSets
}
