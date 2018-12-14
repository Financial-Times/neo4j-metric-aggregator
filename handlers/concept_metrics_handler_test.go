package handlers

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Financial-Times/neo4j-metric-aggregator/concept"
	"github.com/stretchr/testify/mock"
)

var testConceptsUUIDs = []string{
	"38ea6443-050e-4d02-9564-537490f84abd",
	"a4de0e8f-96f4-4ccf-ba26-410f005e021b",
	"e25c0e2c-e275-403b-8fd8-9f079634cae9",
}

var testConcepts = []concept.Concept{
	{
		UUID:    testConceptsUUIDs[0],
		Metrics: concept.Metrics{AnnotationsCount: 1, PrevWeekAnnotationsCount: 2},
	},
	{
		UUID:    testConceptsUUIDs[1],
		Metrics: concept.Metrics{AnnotationsCount: 123, PrevWeekAnnotationsCount: 1024},
	},
	{
		UUID:    testConceptsUUIDs[2],
		Metrics: concept.Metrics{AnnotationsCount: 12, PrevWeekAnnotationsCount: 52},
	},
}

const testJSONPayload = `
[
  {
    "uuid": "38ea6443-050e-4d02-9564-537490f84abd",
    "metrics": {
	  "annotationsCount": 1,
	  "prevWeekAnnotationsCount": 2
    }
  },
  {
    "uuid": "a4de0e8f-96f4-4ccf-ba26-410f005e021b",
    "metrics": {
      "annotationsCount": 123,
	  "prevWeekAnnotationsCount": 1024
    }
  },
  {
    "uuid": "e25c0e2c-e275-403b-8fd8-9f079634cae9",
    "metrics": {
      "annotationsCount": 12,
	  "prevWeekAnnotationsCount": 52
    }
  }
]
`

const testQueryParam = "?uuids=38ea6443-050e-4d02-9564-537490f84abd,a4de0e8f-96f4-4ccf-ba26-410f005e021b,e25c0e2c-e275-403b-8fd8-9f079634cae9"

func TestHappyGetMetrics(t *testing.T) {
	ma := new(MockMetricsAggregator)
	ma.On("GetConceptMetrics", mock.AnythingOfType("*context.valueCtx"), testConceptsUUIDs).Return(testConcepts, nil)

	h := NewConceptsMetricsHandler(ma, 10)
	req := httptest.NewRequest("GET", "http://localhost:8080/concepts/metrics"+testQueryParam, nil)
	w := httptest.NewRecorder()

	h.GetMetrics(w, req)
	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	actualJSONBody, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.JSONEq(t, testJSONPayload, string(actualJSONBody))

	ma.AssertExpectations(t)
}

func TestGetMetricsMissingUUIDsQueryParam(t *testing.T) {
	ma := new(MockMetricsAggregator)

	h := NewConceptsMetricsHandler(ma, 10)
	req := httptest.NewRequest("GET", "http://localhost:8080/concepts/metrics", nil)
	w := httptest.NewRecorder()

	h.GetMetrics(w, req)
	resp := w.Result()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	actualJSONBody, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"message":"uuids URL query parameter is missing or empty"}`, string(actualJSONBody))

	ma.AssertExpectations(t)
}

func TestGetMetricsEmptyUUIDsQueryParam(t *testing.T) {
	ma := new(MockMetricsAggregator)

	h := NewConceptsMetricsHandler(ma, 10)
	req := httptest.NewRequest("GET", "http://localhost:8080/concepts/metrics?uuids=", nil)
	w := httptest.NewRecorder()

	h.GetMetrics(w, req)
	resp := w.Result()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	actualJSONBody, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"message":"uuids URL query parameter is missing or empty"}`, string(actualJSONBody))

	ma.AssertExpectations(t)
}

func TestUUIDsBatchLimit(t *testing.T) {
	ma := new(MockMetricsAggregator)

	h := NewConceptsMetricsHandler(ma, 2)
	req := httptest.NewRequest("GET", "http://localhost:8080/concepts/metrics"+testQueryParam, nil)
	w := httptest.NewRecorder()

	h.GetMetrics(w, req)
	resp := w.Result()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	actualJSONBody, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"message":"max concept UUIDs batch size is 2"}`, string(actualJSONBody))

	ma.AssertExpectations(t)
}

func TestMetricsAggregatorError(t *testing.T) {
	ma := new(MockMetricsAggregator)
	ma.On("GetConceptMetrics", mock.AnythingOfType("*context.valueCtx"), testConceptsUUIDs).Return([]concept.Concept{}, errors.New("computer says no"))

	h := NewConceptsMetricsHandler(ma, 10)
	req := httptest.NewRequest("GET", "http://localhost:8080/concepts/metrics"+testQueryParam, nil)
	w := httptest.NewRecorder()

	h.GetMetrics(w, req)
	resp := w.Result()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	actualJSONBody, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"message":"computer says no"}`, string(actualJSONBody))

	ma.AssertExpectations(t)
}

type MockMetricsAggregator struct {
	mock.Mock
}

func (m *MockMetricsAggregator) GetConceptMetrics(ctx context.Context, conceptUUIDs []string) ([]concept.Concept, error) {
	args := m.Called(ctx, conceptUUIDs)
	return args.Get(0).([]concept.Concept), args.Error(1)
}
