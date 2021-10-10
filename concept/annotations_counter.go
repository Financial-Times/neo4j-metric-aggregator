package concept

import (
	"errors"
	"fmt"
	"time"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
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
	RETURN canonicalConcept.prefUUID AS uuid, count(cl) AS recentCount, totalCount
`

type AnnotationsCounter interface {
	Count(conceptUUIDs []string) (map[string]Metrics, error)
}

func NewAnnotationsCounter(driver *cmneo4j.Driver) AnnotationsCounter {
	return &neoAnnotationsCounter{driver}
}

type neoAnnotationsCounter struct {
	driver *cmneo4j.Driver
}

func (c *neoAnnotationsCounter) Count(conceptUUIDs []string) (map[string]Metrics, error) {
	retval := make(map[string]Metrics)
	queries := buildQueries(conceptUUIDs)

	err := c.driver.Read(queries...)
	if errors.Is(err, cmneo4j.ErrNoResultsFound) {
		// TODO: due to bug in the query most of the queries will fall under this scenario,
		// the bug should be fixed in the query itself.
		return retval, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed executing queries: %w", err)
	}

	for _, q := range queries {
		neoRes := q.Result
		res, ok := neoRes.(*NeoMetricResult)
		if !ok {
			return nil, fmt.Errorf("failed parsing query results: %w", err)
		}
		retval[res.UUID] = Metrics{PrevWeekAnnotationsCount: res.RecentCount, AnnotationsCount: res.TotalCount}
	}

	return retval, nil
}

func buildQueries(conceptUUIDs []string) []*cmneo4j.Query {
	var queries []*cmneo4j.Query

	now := time.Now().Unix()
	weekAgo := now - 7*24*3600

	for _, conceptUUID := range conceptUUIDs {
		res := NeoMetricResult{}
		q := &cmneo4j.Query{
			Cypher: countAnnotationsQuery,
			Params: map[string]interface{}{"uuid": conceptUUID, "since": weekAgo},
			Result: &res,
		}
		queries = append(queries, q)
	}

	return queries
}
