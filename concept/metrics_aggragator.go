package concept

import (
	"fmt"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	log "github.com/sirupsen/logrus"
)

type MetricsAggregator interface {
	GetConceptMetrics(conceptUUIDs []string) ([]Concept, error)
}

func NewMetricsAggregator(driverPool bolt.DriverPool) MetricsAggregator {
	ac := NewAnnotationsCounter(driverPool)
	return &conceptMetricsAggregator{ac}
}

type conceptMetricsAggregator struct {
	annotationsCounter AnnotationsCounter
}

func (a *conceptMetricsAggregator) GetConceptMetrics(conceptUUIDs []string) ([]Concept, error) {
	counts, err := a.annotationsCounter.Count(conceptUUIDs)
	if err != nil {
		log.WithError(err).Error("error in getting annotations count")
		return nil, fmt.Errorf("error in getting annotations count %v", err.Error())
	}

	concepts := []Concept{}

	for _, conceptUUID := range conceptUUIDs {
		count, ok := counts[conceptUUID]
		if ok {
			c := Concept{UUID: conceptUUID}
			c.Metrics = Metrics{AnnotationsCount: count}
			concepts = append(concepts, c)
		}
	}
	return concepts, nil
}
