package main

import (
	"net/http"
	"os"

	"github.com/Financial-Times/http-handlers-go/httphandlers"
	"github.com/Financial-Times/neo4j-metric-aggregator/concept"
	"github.com/Financial-Times/neo4j-metric-aggregator/handlers"
	"github.com/Financial-Times/neo4j-metric-aggregator/health"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/husobee/vestigo"
	"github.com/jawher/mow.cli"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

const (
	systemCode     = "neo4j-metric-aggregator"
	appDescription = "An app to compute metrics on concepts in Neo4j"
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
		Value:  20,
		Desc:   "The maximum number of concepts per request",
		EnvVar: "MAX_REQUEST_BATCH_SIZE",
	})

	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.InfoLevel)
	log.Infof("[Startup] %v is starting", *appSystemCode)

	app.Action = func() {
		log.Infof("System code: %s, App Name: %s, Port: %s", *appSystemCode, *appName, *port)

		driverPool, err := bolt.NewDriverPool(*neo4jEndpoint, *neo4jMaxConnections)
		if err != nil {
			log.WithField("neo4jURL", *neo4jEndpoint).
				WithError(err).
				Fatal("Unable to create a connection pool to neo4j")
		}

		aggregator := concept.NewMetricsAggregator(driverPool)
		h := handlers.NewConceptsMetricsHandler(aggregator, *maxRequestBatchSize)

		healthSvc := health.NewHealthService(*appSystemCode, *appName, appDescription, driverPool)

		serveEndpoints(*port, h, healthSvc)
	}

	app.Run(os.Args)
}

func serveEndpoints(port string, handler *handlers.ConceptsMetricsHandler, healthSvc *health.HealthService) {
	r := vestigo.NewRouter()

	r.Get("/concepts/metrics", handler.GetMetrics)

	var monitoringRouter http.Handler = r
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	http.HandleFunc("/__health", healthSvc.HealthCheckHandleFunc())
	http.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthSvc.GTG))
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	http.Handle("/", monitoringRouter)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Unable to start: %v", err)
	}
}
