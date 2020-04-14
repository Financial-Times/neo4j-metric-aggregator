package healthcheck

import (
	"net/http"
	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/service-status-go/gtg"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	log "github.com/sirupsen/logrus"
)

type HealthService struct {
	fthealth.TimedHealthCheck
	neo4jConnectionsPool bolt.DriverPool
}

func NewHealthService(appSystemCode string, appName string, appDescription string, neo4jConnectionsPool bolt.DriverPool) *HealthService {
	hcService := &HealthService{}
	hcService.neo4jConnectionsPool = neo4jConnectionsPool
	hcService.SystemCode = appSystemCode
	hcService.Name = appName
	hcService.Description = appDescription
	hcService.Timeout = 10 * time.Second
	hcService.Checks = []fthealth.Check{
		hcService.neo4jCheck(),
	}
	return hcService
}

func (service *HealthService) HealthCheckHandleFunc() func(w http.ResponseWriter, r *http.Request) {
	return fthealth.Handler(service)
}

func (service *HealthService) neo4jCheck() fthealth.Check {
	return fthealth.Check{
		ID:               "check-neo4j-healthCheck",
		BusinessImpact:   "No immediate business impact. Concept search may provide reduced quality results.",
		Name:             "Check Neo4J Health",
		PanicGuide:       "https://runbooks.in.ft.com/neo4j-metric-aggregator",
		Severity:         1,
		TechnicalSummary: "App cannot compute concept metrics from Neo4j",
		Checker:          service.neo4jChecker,
	}
}

func (service *HealthService) neo4jChecker() (string, error) {
	conn, err := service.neo4jConnectionsPool.OpenPool()
	if err != nil {
		log.WithError(err).Error("Could not open connections pool for healthcheck")
		return "", err
	}
	defer closeConnection(conn)

	if _, _, _, err = conn.QueryNeoAll(`MATCH (all) RETURN COUNT(all)`, nil); err != nil {
		log.WithError(err).Error("Could not query neo4j for healthcheck")
		return "", err
	}

	return "Neo4J is healthy", nil
}

func closeConnection(conn bolt.Conn) {
	if err := conn.Close(); err != nil {
		log.WithError(err).Error("Could not close neo4j connection for healthcheck")
	}
}

func (service *HealthService) GTG() gtg.Status {
	var checks []gtg.StatusChecker

	for idx := range service.Checks {
		check := service.Checks[idx]

		checks = append(checks, func() gtg.Status {
			if _, err := check.Checker(); err != nil {
				return gtg.Status{GoodToGo: false, Message: err.Error()}
			}
			return gtg.Status{GoodToGo: true}
		})
	}
	return gtg.FailFastParallelCheck(checks)()
}
