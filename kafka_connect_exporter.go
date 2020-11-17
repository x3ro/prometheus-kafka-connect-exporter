package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

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

type connectors []string

type status struct {
	Name      string    `json:"name"`
	Connector connector `json:"connector"`
	Tasks     []task    `json:"tasks"`
}

type connector struct {
	State    string `json:"state"`
	WorkerId string `json:"worker_id"`
}

type task struct {
	State    string  `json:"state"`
	Id       float64 `json:"id"`
	WorkerId string  `json:"worker_id"`
}

type Exporter struct {
	URI string
	up  prometheus.Gauge
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	// Unchecked collector, so `Describe` is intentionally empty
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	client := http.Client{
		Timeout: 3 * time.Second,
	}
	e.up.Set(0)

	response, err := client.Get(e.URI + "/connectors")
	if err != nil {
		log.Errorf("Can't scrape kafka connect: %v", err)
		ch <- e.up
		return
	}
	defer func() {
		err = response.Body.Close()
		if err != nil {
			log.Errorf("Can't close connection to kafka connect: %v", err)
			return
		}
	}()

	output, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Errorf("Can't scrape kafka connect: %v", err)
		ch <- e.up
		return
	}

	var connectorsList connectors
	if err := json.Unmarshal(output, &connectorsList); err != nil {
		log.Errorf("Can't scrape kafka connect: %v", err)
		ch <- e.up
		return
	}

	e.up.Set(1)
	ch <- e.up

	for _, connector := range connectorsList {

		connectorStatusResponse, err := client.Get(e.URI + "/connectors/" + connector + "/status")
		if err != nil {
			log.Errorf("Can't get /status for: %v", err)
			continue
		}

		connectorStatusOutput, err := ioutil.ReadAll(connectorStatusResponse.Body)
		if err != nil {
			log.Errorf("Can't read Body for: %v", err)
			continue
		}

		var connectorStatus status
		if err := json.Unmarshal(connectorStatusOutput, &connectorStatus); err != nil {
			log.Errorf("Can't decode response for: %v", err)
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			isConnectorRunningDesc,
			prometheus.GaugeValue,
			1,
			connectorStatus.Name,
			strings.ToLower(connectorStatus.Connector.State),
			connectorStatus.Connector.WorkerId,
		)

		for _, connectorTask := range connectorStatus.Tasks {
			ch <- prometheus.MustNewConstMetric(
				areConnectorTasksRunningDesc,
				prometheus.GaugeValue,
				1,
				connectorStatus.Name,
				strings.ToLower(connectorTask.State),
				connectorTask.WorkerId,
				fmt.Sprintf("%d", int(connectorTask.Id)),
			)
		}

		err = connectorStatusResponse.Body.Close()
		if err != nil {
			log.Errorf("Can't close connection to connector: %v", err)
		}
	}

	return
}

func newExporter(uri string) *Exporter {
	log.Infoln("Collecting data from:", uri)

	return &Exporter{
		URI: uri,
		up: prometheus.NewGauge(prometheus.GaugeOpts{
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
