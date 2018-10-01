package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/Financial-Times/neo4j-metric-aggregator/concept"
	tidUtils "github.com/Financial-Times/transactionid-utils-go"
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
	tid := tidUtils.GetTransactionIDFromRequest(r)
	ctx := tidUtils.TransactionAwareContext(context.Background(), tid)

	w.Header().Add("Content-Type", "application/json")

	uuids, err := h.extractConceptUUIDs(r)
	if err != nil {
		writeJSONError(w, err, http.StatusBadRequest)
		writeRequestLog(r, tid, http.StatusBadRequest)
		return
	}

	concepts, err := h.metricsAggregator.GetConceptMetrics(ctx, uuids)
	if err != nil {
		writeJSONError(w, err, http.StatusInternalServerError)
		writeRequestLog(r, tid, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&concepts)
	writeRequestLog(r, tid, http.StatusOK)
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

func writeJSONError(w http.ResponseWriter, err error, status int) {
	w.WriteHeader(status)

	message := make(map[string]interface{})
	message["message"] = err.Error()
	j, err := json.Marshal(&message)
	if err != nil {
		log.WithError(err).Error("Failed to parse provided message to json, this is a bug.")
		return
	}

	w.Write(j)
}

func writeRequestLog(req *http.Request, transactionID string, status int) {
	username := "-"
	if req.URL.User != nil {
		if name := req.URL.User.Username(); name != "" {
			username = name
		}
	}

	host, _, err := net.SplitHostPort(req.RemoteAddr)

	if err != nil {
		host = req.RemoteAddr
	}

	req.URL.RawQuery = ""
	uri := req.URL.RequestURI()

	// Requests using the CONNECT method over HTTP/2.0 must use
	// the authority field (aka r.Host) to identify the target.
	// Refer: https://httpwg.github.io/specs/rfc7540.html#CONNECT
	if req.ProtoMajor == 2 && req.Method == "CONNECT" {
		uri = req.Host
	}

	log.WithFields(log.Fields{
		"host":           host,
		"username":       username,
		"method":         req.Method,
		"transaction_id": transactionID,
		"uri":            uri,
		"protocol":       req.Proto,
		"status":         status,
		"referer":        req.Referer(),
		"userAgent":      req.UserAgent(),
	}).Info()

}
