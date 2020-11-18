# Prometheus Kafka Connect exporter

A [Prometheus](https://prometheus.io/) exporter that collects [Kafka connect](https://docs.confluent.io/current/connect/index.html) metrics.

### Usage

```sh
$ ./kafka_connect_exporter -h
Usage of ./kafka_connect_exporter:
  -listen-address string
        Address on which to expose metrics. (default ":8080")
  -scrape-uri string
        URI on which to scrape kafka connect. (default "http://127.0.0.1:8080")
  -telemetry-path string
        Path under which to expose metrics. (default "/metrics")
  -version
        show version and exit
```
