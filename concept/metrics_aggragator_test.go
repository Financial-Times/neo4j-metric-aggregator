package concept

import (
	"context"
	"testing"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGetConceptMetrics(t *testing.T) {

	conceptUuids := []string{
		"601a5957-74ab-4eab-8a43-4596355c9420",
		"082a9fcc-5a88-48c5-bd60-64ba154204df",
		"f7885509-c029-496b-87dd-aecf1ca138d7",
	}

	countResult := map[string]Metrics{
		"601a5957-74ab-4eab-8a43-4596355c9420": Metrics{3, 5},
		"082a9fcc-5a88-48c5-bd60-64ba154204df": Metrics{123, 1000},
		"f7885509-c029-496b-87dd-aecf1ca138d7": Metrics{4, 1024},
	}

	ma := new(conceptMetricsAggregator)
	ac := new(MockAnnotationCounter)
	ac.On("Count", conceptUuids).Return(countResult, nil)
	ma.annotationsCounter = ac

	expectedConcepts := []Concept{
		{
			"601a5957-74ab-4eab-8a43-4596355c9420",
			Metrics{3, 5},
		},
		{
			"082a9fcc-5a88-48c5-bd60-64ba154204df",
			Metrics{123, 1000},
		},
		{
			"f7885509-c029-496b-87dd-aecf1ca138d7",
			Metrics{4, 1024},
		},
	}

	actualConcepts, err := ma.GetConceptMetrics(context.Background(), conceptUuids)
	assert.NoError(t, err)
	assert.Equal(t, expectedConcepts, actualConcepts)
	ac.AssertExpectations(t)
}

func TestGetConceptMetricsWithMissingResults(t *testing.T) {

	conceptUuids := []string{
		"601a5957-74ab-4eab-8a43-4596355c9420",
		"082a9fcc-5a88-48c5-bd60-64ba154204df",
		"f7885509-c029-496b-87dd-aecf1ca138d7",
	}

	countResult := map[string]Metrics{
		"601a5957-74ab-4eab-8a43-4596355c9420": Metrics{3, 113},
		"f7885509-c029-496b-87dd-aecf1ca138d7": Metrics{4, 1024},
	}

	ma := new(conceptMetricsAggregator)
	ac := new(MockAnnotationCounter)
	ac.On("Count", conceptUuids).Return(countResult, nil)
	ma.annotationsCounter = ac

	expectedConcepts := []Concept{
		{
			"601a5957-74ab-4eab-8a43-4596355c9420",
			Metrics{3, 113},
		},
		{
			"f7885509-c029-496b-87dd-aecf1ca138d7",
			Metrics{4, 1024},
		},
	}

	actualConcepts, err := ma.GetConceptMetrics(context.Background(), conceptUuids)
	assert.NoError(t, err)
	assert.Equal(t, expectedConcepts, actualConcepts)
	ac.AssertExpectations(t)
}

func TestGetConceptMetricsWithNoResults(t *testing.T) {

	conceptUuids := []string{
		"601a5957-74ab-4eab-8a43-4596355c9420",
		"082a9fcc-5a88-48c5-bd60-64ba154204df",
		"f7885509-c029-496b-87dd-aecf1ca138d7",
	}

	ma := new(conceptMetricsAggregator)
	ac := new(MockAnnotationCounter)
	ac.On("Count", conceptUuids).Return(map[string]Metrics{}, nil)
	ma.annotationsCounter = ac

	expectedConcepts := []Concept{}

	actualConcepts, err := ma.GetConceptMetrics(context.Background(), conceptUuids)
	assert.NoError(t, err)
	assert.Equal(t, expectedConcepts, actualConcepts)
	ac.AssertExpectations(t)
}

func TestGetConceptMetricsError(t *testing.T) {

	conceptUuids := []string{
		"601a5957-74ab-4eab-8a43-4596355c9420",
		"082a9fcc-5a88-48c5-bd60-64ba154204df",
		"f7885509-c029-496b-87dd-aecf1ca138d7",
	}

	ma := new(conceptMetricsAggregator)
	ac := new(MockAnnotationCounter)
	ac.On("Count", conceptUuids).Return(map[string]Metrics{}, errors.New("computer says no"))
	ma.annotationsCounter = ac

	_, err := ma.GetConceptMetrics(context.Background(), conceptUuids)
	assert.Error(t, err)
	ac.AssertExpectations(t)
}

type MockAnnotationCounter struct {
	mock.Mock
}

func (m *MockAnnotationCounter) Count(conceptUUIDs []string) (map[string]Metrics, error) {
	args := m.Called(conceptUUIDs)
	return args.Get(0).(map[string]Metrics), args.Error(1)
}
