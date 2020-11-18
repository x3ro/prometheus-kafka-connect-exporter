package main

import (
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
)

const nameSpace = "kafka_connect"

var (
	version    = "dev"
	versionUrl = "https://github.com/wakeful/kafka_connect_exporter"

	showVersion   = flag.Bool("version", false, "show version and exit")
	listenAddress = flag.String("listen-address", ":8080", "Address on which to expose metrics.")
	metricsPath   = flag.String("telemetry-path", "/metrics", "Path under which to expose metrics.")
	scrapeURI     = flag.String("scrape-uri", "http://127.0.0.1:8080", "URI on which to scrape kafka connect.")

	isConnectorRunningDesc = prometheus.NewDesc(
		prometheus.BuildFQName(nameSpace, "connector", "state"),
		"Is the connector up?",
		[]string{"connector", "state", "worker_id"},
		nil)

	areConnectorTasksRunningDesc = prometheus.NewDesc(
		prometheus.BuildFQName(nameSpace, "connector_task", "state"),
		"Are the tasks for the connector up?",
		[]string{"connector", "state", "worker_id", "task_id"},
		nil)
)

type collector struct {
	URI     string
	upGauge prometheus.Gauge
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	// Unchecked collector, so `Describe` is intentionally empty
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	connectClient := connectClient{
		URI: c.URI,
	}

	connectorList, err := connectClient.fetchConnectors()
	if err != nil {
		c.upGauge.Set(0)
		ch <- c.upGauge
		log.Errorf("Failed to fetch connector list, aborting: %w", err)
		return
	}

	for _, connectorName := range connectorList {
		status, err := connectClient.fetchConnectorStatus(connectorName)
		if err != nil {
			c.upGauge.Set(0)
			ch <- c.upGauge
			log.Errorf("Failed to fetch connector status for '%s': %w", connectorName, err)
			return
		}

		ch <- prometheus.MustNewConstMetric(
			isConnectorRunningDesc,
			prometheus.GaugeValue,
			1,
			status.Name,
			strings.ToLower(status.Connector.State),
			status.Connector.WorkerId,
		)

		for _, task := range status.Tasks {
			ch <- prometheus.MustNewConstMetric(
				areConnectorTasksRunningDesc,
				prometheus.GaugeValue,
				1,
				status.Name,
				strings.ToLower(task.State),
				task.WorkerId,
				fmt.Sprintf("%d", int(task.Id)),
			)
		}
	}

	c.upGauge.Set(1)
	ch <- c.upGauge

	return
}

func newExporter(uri string) *collector {
	log.Infoln("Collecting data from:", uri)

	return &collector{
		URI: uri,
		upGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: nameSpace,
			Name:      "up",
			Help:      "Was the last scrape of kafka connect successful?",
		}),
	}

}

var supportedSchema = map[string]bool{
	"http":  true,
	"https": true,
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("kafka_connect_exporter\n url: %s\n version: %s\n", versionUrl, version)
		os.Exit(2)
	}

	parseURI, err := url.Parse(*scrapeURI)
	if err != nil {
		log.Errorf("%v", err)
		os.Exit(1)
	}
	if !supportedSchema[parseURI.Scheme] {
		log.Error("schema not supported")
		os.Exit(1)
	}

	log.Infoln("Starting kafka_connect_exporter")

	prometheus.Unregister(prometheus.NewGoCollector())
	prometheus.Unregister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	prometheus.MustRegister(newExporter(*scrapeURI))

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, *metricsPath, http.StatusMovedPermanently)
	})

	log.Fatal(http.ListenAndServe(*listenAddress, nil))

}
