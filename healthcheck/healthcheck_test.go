// +build integration

package healthcheck

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	logger "github.com/Financial-Times/go-logger/v2"
	status "github.com/Financial-Times/service-status-go/httphandlers"
)

func TestHappyHealthCheck(t *testing.T) {
	log := logger.NewUPPLogger("test-neo4j-metric-aggregator", "warning")
	neoTestURL := getNeoTestURL(t)
	d, err := cmneo4j.NewDefaultDriver(neoTestURL, log)
	require.NoError(t, err)

	h := NewHealthService("", "", "", d)

	req := httptest.NewRequest("GET", "/__health", nil)
	w := httptest.NewRecorder()
	h.HealthCheckHandleFunc()(w, req)

	resp := w.Result()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result fthealth.HealthResult
	err = json.NewDecoder(resp.Body).Decode(&result)

	assert.NoError(t, err)
	assert.Len(t, result.Checks, 1)
	assert.True(t, result.Ok)

	assert.True(t, result.Checks[0].Ok)
	assert.Equal(t, "Neo4J is healthy", result.Checks[0].CheckOutput)
	assert.Equal(t, "App cannot compute concept metrics from Neo4j", result.Checks[0].TechnicalSummary)
}

func TestUnhappyHealthCheck(t *testing.T) {
	log := logger.NewUPPLogger("test-neo4j-metric-aggregator", "warning")
	d, err := cmneo4j.NewDefaultDriver("bolt://localhost:80", log)
	require.NoError(t, err)

	h := NewHealthService("", "", "", d)

	req := httptest.NewRequest("GET", "/__health", nil)
	w := httptest.NewRecorder()
	h.HealthCheckHandleFunc()(w, req)

	resp := w.Result()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result fthealth.HealthResult
	err = json.NewDecoder(resp.Body).Decode(&result)

	assert.NoError(t, err)
	assert.Len(t, result.Checks, 1)
	assert.False(t, result.Ok)

	assert.False(t, result.Checks[0].Ok)
	assert.NotEqual(t, "Neo4J is healthy", result.Checks[0].CheckOutput)
	assert.Equal(t, "App cannot compute concept metrics from Neo4j", result.Checks[0].TechnicalSummary)
}

func TestHappyGTG(t *testing.T) {
	log := logger.NewUPPLogger("test-neo4j-metric-aggregator", "warning")
	neoTestURL := getNeoTestURL(t)
	d, err := cmneo4j.NewDefaultDriver(neoTestURL, log)
	require.NoError(t, err)

	h := NewHealthService("", "", "", d)

	req := httptest.NewRequest("GET", "/__gtg", nil)
	w := httptest.NewRecorder()
	status.NewGoodToGoHandler(h.GTG)(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestUnhappyGTG(t *testing.T) {
	log := logger.NewUPPLogger("test-neo4j-metric-aggregator", "warning")
	d, err := cmneo4j.NewDefaultDriver("bolt://localhost:80", log)
	require.NoError(t, err)

	h := NewHealthService("", "", "", d)

	req := httptest.NewRequest("GET", "/__gtg", nil)
	w := httptest.NewRecorder()
	status.NewGoodToGoHandler(h.GTG)(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func getNeoTestURL(t *testing.T) string {
	if testing.Short() {
		t.Skip("Skipping Neo4j integration tests.")
		return ""
	}

	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "bolt://localhost:7687"
	}

	return url
}
