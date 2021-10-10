package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	log "github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/neo4j-metric-aggregator/concept"
	tidUtils "github.com/Financial-Times/transactionid-utils-go"
)

type ConceptsMetricsHandler struct {
	metricsAggregator concept.MetricsAggregator
	maxUUIDBatchSize  int
	log               *log.UPPLogger
}

func NewConceptsMetricsHandler(metricsAggregator concept.MetricsAggregator, maxUUIDBatchSize int, log *log.UPPLogger) *ConceptsMetricsHandler {
	return &ConceptsMetricsHandler{
		metricsAggregator: metricsAggregator,
		maxUUIDBatchSize:  maxUUIDBatchSize,
		log:               log,
	}
}

func (h *ConceptsMetricsHandler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	tid := tidUtils.GetTransactionIDFromRequest(r)
	ctx := tidUtils.TransactionAwareContext(context.Background(), tid)

	w.Header().Add("Content-Type", "application/json")

	uuids, err := h.extractConceptUUIDs(r)
	if err != nil {
		h.writeJSONError(w, err, http.StatusBadRequest)
		h.writeRequestLog(r, tid, http.StatusBadRequest)
		return
	}

	concepts, err := h.metricsAggregator.GetConceptMetrics(ctx, uuids)
	if err != nil {
		h.writeJSONError(w, err, http.StatusInternalServerError)
		h.writeRequestLog(r, tid, http.StatusInternalServerError)
		return
	}

	if err = json.NewEncoder(w).Encode(&concepts); err != nil {
		h.writeJSONError(w, err, http.StatusInternalServerError)
		h.writeRequestLog(r, tid, http.StatusInternalServerError)
		return
	}

	h.writeRequestLog(r, tid, http.StatusOK)
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

func (h *ConceptsMetricsHandler) writeJSONError(w http.ResponseWriter, err error, status int) {
	w.WriteHeader(status)

	message := make(map[string]interface{})
	message["message"] = err.Error()
	j, err := json.Marshal(&message)
	if err != nil {
		h.log.WithError(err).Error("Failed to parse provided message to json, this is a bug.")
		return
	}

	if _, err := w.Write(j); err != nil {
		h.log.WithError(err).Error("Failed to write json data to response")
		return
	}
}

func (h *ConceptsMetricsHandler) writeRequestLog(req *http.Request, transactionID string, status int) {
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

	h.log.WithFields(map[string]interface{}{
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
