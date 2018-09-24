package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Financial-Times/neo4j-metric-aggregator/concept"
	log "github.com/sirupsen/logrus"
)

type ConceptsMetricsHandler struct {
	metricsAggregator concept.MetricsAggregator
	maxUUIDBatchSize  int
}

func NewConceptsMetricsHandler(metricsAggregator concept.MetricsAggregator, maxUUIDBatchSize int) *ConceptsMetricsHandler {
	return &ConceptsMetricsHandler{metricsAggregator, maxUUIDBatchSize}
}

func (h *ConceptsMetricsHandler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	uuids, err := h.extractConceptUUIDs(r)
	if err != nil {
		writeJSONMessage(w, err.Error(), http.StatusBadRequest)
		return
	}

	concepts, err := h.metricsAggregator.GetConceptMetrics(uuids)
	if err != nil {
		writeJSONMessage(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&concepts)
}

func (h *ConceptsMetricsHandler) extractConceptUUIDs(r *http.Request) ([]string, error) {
	commaSeparatedUUIDs := r.URL.Query().Get("uuids")
	if commaSeparatedUUIDs == "" {
		return nil, errors.New("uuids URL query parameter is missing or empty")
	}
	uuids := strings.Split(commaSeparatedUUIDs, ",")
	if len(uuids) > h.maxUUIDBatchSize {
		return nil, fmt.Errorf("max concept UUIDs batch size is %v", h.maxUUIDBatchSize)
	}
	return uuids, nil
}

func writeJSONMessage(w http.ResponseWriter, msg string, status int) {
	w.WriteHeader(status)

	message := make(map[string]interface{})
	message["message"] = msg
	j, err := json.Marshal(&message)

	if err != nil {
		log.WithError(err).Error("Failed to parse provided message to json, this is a bug.")
		return
	}

	w.Write(j)
}
