package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	cli "github.com/jawher/mow.cli"
	metrics "github.com/rcrowley/go-metrics"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	logger "github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/http-handlers-go/v2/httphandlers"
	"github.com/Financial-Times/neo4j-metric-aggregator/concept"
	"github.com/Financial-Times/neo4j-metric-aggregator/handlers"
	"github.com/Financial-Times/neo4j-metric-aggregator/healthcheck"
	status "github.com/Financial-Times/service-status-go/httphandlers"
)

const (
	systemCode     = "neo4j-metric-aggregator"
	appDescription = "An app to compute metrics on Neo4j knowledge base"

	httpServerReadTimeout  = 10 * time.Second
	httpServerWriteTimeout = 15 * time.Second
	httpServerIdleTimeout  = 20 * time.Second
	httpHandlersTimeout    = 14 * time.Second
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

	maxRequestBatchSize := app.Int(cli.IntOpt{
		Name:   "maxRequestBatchSize",
		Value:  1000,
		Desc:   "The maximum number of concepts per request",
		EnvVar: "MAX_REQUEST_BATCH_SIZE",
	})

	log := logger.NewUPPInfoLogger(*appName)
	dbLog := logger.NewUPPLogger(fmt.Sprintf("%s %s", *appName, "cmneo4j-driver"), "warning")

	app.Action = func() {
		log.WithFields(map[string]interface{}{
			"appName":             *appName,
			"appSystemCode":       *appSystemCode,
			"port":                *port,
			"neo4jEndpoint":       *neo4jEndpoint,
			"maxRequestBatchSize": *maxRequestBatchSize,
		}).Infof("[Startup] %v is starting", *appSystemCode)

		neoDriver, err := cmneo4j.NewDefaultDriver(*neo4jEndpoint, dbLog)
		if err != nil {
			log.WithField("neo4jURL", *neo4jEndpoint).
				WithError(err).
				Fatal("Could not initiate cmneo4j driver")
		}

		aggregator := concept.NewMetricsAggregator(neoDriver, log)
		h := handlers.NewConceptsMetricsHandler(aggregator, *maxRequestBatchSize, log)

		healthSvc := healthcheck.NewHealthService(*appSystemCode, *appName, appDescription, neoDriver)

		router := registerEndpoints(h, healthSvc, log)

		server := newHTTPServer(*port, router)
		go startHTTPServer(server, log)

		waitForSignal()
		stopHTTPServer(server, log)
	}

	if err := app.Run(os.Args); err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}

}

func registerEndpoints(handler *handlers.ConceptsMetricsHandler, healthService *healthcheck.HealthService, log *logger.UPPLogger) http.Handler {
	serveMux := http.NewServeMux()

	// register supervisory endpoint that does not require logging and metrics collection
	serveMux.HandleFunc("/__health", healthService.HealthCheckHandleFunc())
	serveMux.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.GTG))
	serveMux.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	// add services router and register endpoints specific to this service only
	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/concepts/metrics", handler.GetMetrics).Methods("GET")

	// wrap the handlers with certain middlewares providing logging of the requests,
	// sending metrics and handler time out on certain time interval
	var wrappedServicesRouter http.Handler = servicesRouter
	wrappedServicesRouter = httphandlers.TransactionAwareRequestLoggingHandler(log, wrappedServicesRouter)
	wrappedServicesRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, wrappedServicesRouter)
	wrappedServicesRouter = http.TimeoutHandler(wrappedServicesRouter, httpHandlersTimeout, "")

	serveMux.Handle("/", wrappedServicesRouter)

	return serveMux
}

func newHTTPServer(port string, router http.Handler) *http.Server {
	return &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  httpServerReadTimeout,
		WriteTimeout: httpServerWriteTimeout,
		IdleTimeout:  httpServerIdleTimeout,
	}
}

func startHTTPServer(server *http.Server, log *logger.UPPLogger) {
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server failed to start: %v", err)
	}
}

func stopHTTPServer(server *http.Server, log *logger.UPPLogger) {
	log.Info("HTTP server is shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Failed to gracefully shutdown the server: %v", err)
	}
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
