package concept

import (
	"context"
	"fmt"

	tidUtils "github.com/Financial-Times/transactionid-utils-go"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	log "github.com/sirupsen/logrus"
)

type MetricsAggregator interface {
	GetConceptMetrics(ctx context.Context, conceptUUIDs []string) ([]Concept, error)
}

func NewMetricsAggregator(driverPool bolt.DriverPool) MetricsAggregator {
	ac := NewAnnotationsCounter(driverPool)
	return &conceptMetricsAggregator{ac}
}

type conceptMetricsAggregator struct {
	annotationsCounter AnnotationsCounter
}

func (a *conceptMetricsAggregator) GetConceptMetrics(ctx context.Context, conceptUUIDs []string) ([]Concept, error) {
	logRead := log.
		WithField(tidUtils.TransactionIDKey, ctx.Value(tidUtils.TransactionIDKey)).
		WithField("batchSize", len(conceptUUIDs))

	logRead.Info("computing annotations count for concept batch")
	counts, err := a.annotationsCounter.Count(conceptUUIDs)

	if err != nil {
		logRead.WithError(err).Error("error in getting annotations count for batch")
		return nil, fmt.Errorf("error in getting annotations count: %v", err.Error())
	}

	concepts := []Concept{}

	for _, conceptUUID := range conceptUUIDs {
		stats, ok := counts[conceptUUID]
		if ok {
			c := Concept{UUID: conceptUUID}
			c.Metrics = Metrics{AnnotationsCount: stats}
			concepts = append(concepts, c)
		}
	}
	return concepts, nil
}
