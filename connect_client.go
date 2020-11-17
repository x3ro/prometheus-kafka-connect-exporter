package main

import (
	"encoding/json"
	"fmt"
	"github.com/prometheus/common/log"
	"io/ioutil"
	"net/http"
	"time"
)

var client = http.Client{
	Timeout: 3 * time.Second,
}

type connectClient struct {
	URI string
}

type connectorStatus struct {
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

func (c *connectClient) fetchConnectors() ([]string, error) {
	response, err := client.Get(c.URI + "/connectors")
	if err != nil {
		return nil, fmt.Errorf("Failed to fetch connector list: %w", err)
	}
	defer func() {
		err = response.Body.Close()
		if err != nil {
			log.Errorf("Failed to close connection to kafka connect: %v", err)
		}
	}()

	output, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read response body: %w", err)
	}

	var connectors []string
	if err := json.Unmarshal(output, &connectors); err != nil {
		return nil, fmt.Errorf("Failed to parse JSON response: %w", err)
	}

	return connectors, nil
}

func (c *connectClient) fetchConnectorStatus(connectorName string) (connectorStatus, error) {
	var status connectorStatus

	response, err := client.Get(c.URI + "/connectors/" + connectorName + "/status")
	if err != nil {
		return status, fmt.Errorf("Can't get /status for: %w", err)
	}

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return status, fmt.Errorf("Can't read Body for: %w", err)
	}
	defer func() {
		err = response.Body.Close()
		if err != nil {
			log.Errorf("Failed to close connection to kafka connect: %v", err)
		}
	}()

	if err := json.Unmarshal(responseBody, &status); err != nil {
		return status, fmt.Errorf("Can't decode response for: %w", err)
	}

	return status, nil
}
