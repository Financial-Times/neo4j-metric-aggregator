package concept

import (
	"errors"
	"fmt"
	"time"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
)

const countAnnotationsQuery = `
	OPTIONAL MATCH (canonicalConcept:Concept{prefUUID:$uuid})<-[:EQUIVALENT_TO]-(source:Concept)
	OPTIONAL MATCH (source)<-[]-(content:Content)
	WITH canonicalConcept, count(DISTINCT(content)) AS totalCount, COLLECT(DISTINCT(content)) as contentList
	WITH canonicalConcept, totalCount, [x in contentList where x.publishedDateEpoch > $since] as recent
	RETURN CASE canonicalConcept WHEN NULL THEN '' ELSE canonicalConcept.prefUUID END AS uuid, size(recent) AS recentCount, totalCount
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

// Count returns metrics for the given concept uuids list. If given uuid is not found in the db, it is skipped
// from the result map.
func (c *neoAnnotationsCounter) Count(conceptUUIDs []string) (map[string]Metrics, error) {
	retval := make(map[string]Metrics)
	queries := buildQueries(conceptUUIDs)

	err := c.driver.Read(queries...)
	if errors.Is(err, cmneo4j.ErrNoResultsFound) {
		// The defined query uses OPTIONAL MATCH-es and shouldn't return cmneo4j.ErrNoResultsFound,
		// unexpected error happen.
		return nil, fmt.Errorf("unexpected 'no result' returned from the DB: %w", err)
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
		if res.UUID == "" {
			continue
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
