package concept

import (
	"context"
	"fmt"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	log "github.com/Financial-Times/go-logger/v2"
	tidUtils "github.com/Financial-Times/transactionid-utils-go"
)

type MetricsAggregator interface {
	GetConceptMetrics(ctx context.Context, conceptUUIDs []string) ([]Concept, error)
}

func NewMetricsAggregator(driver *cmneo4j.Driver, log *log.UPPLogger) MetricsAggregator {
	ac := NewAnnotationsCounter(driver)

	return &conceptMetricsAggregator{
		annotationsCounter: ac,
		log:                log,
	}
}

type conceptMetricsAggregator struct {
	annotationsCounter AnnotationsCounter
	log                *log.UPPLogger
}

func (a *conceptMetricsAggregator) GetConceptMetrics(ctx context.Context, conceptUUIDs []string) ([]Concept, error) {
	logRead := a.log.
		WithField(tidUtils.TransactionIDKey, ctx.Value(tidUtils.TransactionIDKey)).
		WithField("batchSize", len(conceptUUIDs))

	logRead.Info("computing annotations count for concept batch")
	counts, err := a.annotationsCounter.Count(conceptUUIDs)

	if err != nil {
		logRead.WithError(err).Error("error in getting annotations count for batch")
		return nil, fmt.Errorf("error in getting annotations count: %w", err)
	}

	concepts := []Concept{}

	for _, conceptUUID := range conceptUUIDs {
		metrics, ok := counts[conceptUUID]
		if ok {
			c := Concept{conceptUUID, metrics}
			concepts = append(concepts, c)
		}
	}
	return concepts, nil
}
