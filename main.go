package main

import (
	"net/http"
	"os"

	"github.com/husobee/vestigo"
	cli "github.com/jawher/mow.cli"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"

	logger "github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/neo4j-metric-aggregator/concept"
	"github.com/Financial-Times/neo4j-metric-aggregator/handlers"
	"github.com/Financial-Times/neo4j-metric-aggregator/healthcheck"
	status "github.com/Financial-Times/service-status-go/httphandlers"
)

const (
	systemCode     = "neo4j-metric-aggregator"
	appDescription = "An app to compute metrics on Neo4j knowledge base"
)

func main() {
	app := cli.App(systemCode, appDescription)

	appSystemCode := app.String(cli.StringOpt{
		Name:   "app-system-code",
		Value:  systemCode,
		Desc:   "System Code of the application",
		EnvVar: "APP_SYSTEM_CODE",
	})

	appName := app.String(cli.StringOpt{
		Name:   "app-name",
		Value:  systemCode,
		Desc:   "Application name",
		EnvVar: "APP_NAME",
	})

	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})

	neo4jEndpoint := app.String(cli.StringOpt{
		Name:   "neo4j-endpoint",
		Value:  "bolt://localhost:7687",
		Desc:   "URL of the Neo4j bolt endpoint",
		EnvVar: "NEO4J_ENDPOINT",
	})

	neo4jMaxConnections := app.Int(cli.IntOpt{
		Name:   "neo4j-max-connections",
		Value:  10,
		Desc:   "The maximum number of parallel connections to Neo4J",
		EnvVar: "NEO4J_MAX_CONNECTIONS",
	})

	maxRequestBatchSize := app.Int(cli.IntOpt{
		Name:   "maxRequestBatchSize",
		Value:  1000,
		Desc:   "The maximum number of concepts per request",
		EnvVar: "MAX_REQUEST_BATCH_SIZE",
	})

	log := logger.NewUPPInfoLogger(*appName)
	log.WithFields(map[string]interface{}{
		"appName":             *appName,
		"appSystemCode":       *appSystemCode,
		"port":                *port,
		"neo4jEndpoint":       *neo4jEndpoint,
		"maxRequestBatchSize": *maxRequestBatchSize,
	}).Infof("[Startup] %v is starting", *appSystemCode)

	app.Action = func() {
		driverPool, err := bolt.NewDriverPool(*neo4jEndpoint, *neo4jMaxConnections)
		if err != nil {
			log.WithField("neo4jURL", *neo4jEndpoint).
				WithError(err).
				Fatal("Unable to create a connection pool to neo4j")
		}

		aggregator := concept.NewMetricsAggregator(driverPool, log)
		h := handlers.NewConceptsMetricsHandler(aggregator, *maxRequestBatchSize, log)

		healthSvc := healthcheck.NewHealthService(*appSystemCode, *appName, appDescription, driverPool, log)

		if err = serveEndpoints(*port, h, healthSvc); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Unable to start: %v", err)
		}
	}

	if err := app.Run(os.Args); err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}

}

func serveEndpoints(port string, handler *handlers.ConceptsMetricsHandler, healthSvc *healthcheck.HealthService) error {
	r := vestigo.NewRouter()

	r.Get("/concepts/metrics", handler.GetMetrics)

	http.HandleFunc("/__health", healthSvc.HealthCheckHandleFunc())
	http.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthSvc.GTG))
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	http.Handle("/", r)

	return http.ListenAndServe(":"+port, nil)
}
