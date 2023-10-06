package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	daskv1alpha1 "github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1"
)

func schedulerUrl(cluster *daskv1alpha1.Cluster, path string) string {
	return fmt.Sprintf("http://%s-scheduler.%s.svc/%s", cluster.Name, cluster.Namespace, path)
}

type SchedulerClient interface {
	retireWorkers(cluster *daskv1alpha1.Cluster, n int) ([]daskv1alpha1.RetiredWorker, error)
}

type HttpSchedulerClient struct {
	httpclient http.Client
}

func (c *HttpSchedulerClient) retireWorkers(cluster *daskv1alpha1.Cluster, n int) ([]daskv1alpha1.RetiredWorker, error) {
	params, err := json.Marshal(map[string]interface{}{"n": n})
	if err != nil {
		return nil, err
	}
	response, err := c.httpclient.Post(schedulerUrl(cluster, "api/v1/retire_workers"), "application/json", bytes.NewBuffer(params))
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("error retiring workers: %s", body)
	}

	data := make([]daskv1alpha1.RetiredWorker, n)
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	return data, nil
}
