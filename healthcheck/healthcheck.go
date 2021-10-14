package healthcheck

import (
	"fmt"
	"net/http"
	"time"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/service-status-go/gtg"
)

type HealthService struct {
	fthealth.TimedHealthCheck
	neo4jDriver *cmneo4j.Driver
}

func NewHealthService(appSystemCode string, appName string, appDescription string, neo4jDriver *cmneo4j.Driver) *HealthService {
	hcService := &HealthService{}
	hcService.neo4jDriver = neo4jDriver
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
	err := service.neo4jDriver.VerifyConnectivity()
	if err != nil {
		return fmt.Sprintf("Neo4j connectivity error: %v", err), err
	}

	return "Neo4J is healthy", nil
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
