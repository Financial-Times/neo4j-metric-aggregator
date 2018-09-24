package concept

import (
	"fmt"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

const countAnnotationsQuery = `
	 MATCH (canonicalConcept:Concept{prefUUID:{uuid}})<-[:EQUIVALENT_TO]-(x:Concept)
     OPTIONAL MATCH (x)-[]-(content:Content)
	 RETURN canonicalConcept.prefUUID, count(content)
`

type AnnotationsCounter interface {
	Count(conceptUUIDs []string) (map[string]int64, error)
}

func NewAnnotationsCounter(driverPool bolt.DriverPool) AnnotationsCounter {
	return &neoAnnotationsCounter{driverPool}
}

type neoAnnotationsCounter struct {
	driverPool bolt.DriverPool
}

func (c *neoAnnotationsCounter) Count(conceptUUIDs []string) (map[string]int64, error) {
	conn, err := c.driverPool.OpenPool()
	if err != nil {
		return nil, fmt.Errorf("error in creating a connection to Neo4j: %v", err.Error())
	}
	defer conn.Close()

	queries, parameterSets := buildAnnotationsCountPipelineComponents(conceptUUIDs)
	rows, err := conn.QueryPipeline(queries, parameterSets...)
	if err != nil {
		return nil, fmt.Errorf("error in executing query pipeline in Neo4j %v", err.Error())
	}
	counts := make(map[string]int64)

	var row []interface{}
	var nextPipelineRows bolt.PipelineRows

	for rows != nil {
		row, _, nextPipelineRows, err = rows.NextPipeline()
		if err != nil {
			return nil, fmt.Errorf("error in parsing query reults %v", err.Error())
		}
		if row == nil {
			rows = nextPipelineRows
			continue
		}
		conceptUUID, ok := row[0].(string)
		if ok {
			counts[conceptUUID] = row[1].(int64)
		}
	}

	return counts, nil
}

func buildAnnotationsCountPipelineComponents(conceptUUIDs []string) ([]string, []map[string]interface{}) {
	var queries []string
	var parameterSets []map[string]interface{}
	for _, uuid := range conceptUUIDs {
		queries = append(queries, countAnnotationsQuery)
		params := map[string]interface{}{"uuid": uuid}
		parameterSets = append(parameterSets, params)
	}
	return queries, parameterSets
}
