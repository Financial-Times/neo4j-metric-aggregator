// +build integration

package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHappyHealthCheck(t *testing.T) {
	neoTestURL := getNeoTestURL(t)
	dp, err := bolt.NewDriverPool(neoTestURL, 10)
	require.NoError(t, err)
	h := NewHealthService("", "", "", dp)

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
	dp, err := bolt.NewDriverPool("bolt://localhost:80", 10)
	require.NoError(t, err)
	h := NewHealthService("", "", "", dp)

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
	neoTestURL := getNeoTestURL(t)
	dp, err := bolt.NewDriverPool(neoTestURL, 10)
	require.NoError(t, err)

	h := NewHealthService("", "", "", dp)
	req := httptest.NewRequest("GET", "/__gtg", nil)
	w := httptest.NewRecorder()
	status.NewGoodToGoHandler(h.GTG)(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestUnhappyGTG(t *testing.T) {
	dp, err := bolt.NewDriverPool("bolt://localhost:80", 10)
	require.NoError(t, err)

	h := NewHealthService("", "", "", dp)
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
