package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	daskv1alpha1 "github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1"
	"k8s.io/klog/v2"
)

func schedulerUrl(cluster *daskv1alpha1.Cluster, path string) string {
	return fmt.Sprintf("http://%s-scheduler.%s.svc:8787/%s", cluster.Name, cluster.Namespace, path)
}

type RetireResult map[string]struct{ id string }

type SchedulerClient interface {
	retireWorkers(cluster *daskv1alpha1.Cluster, n int) (RetireResult, error)
}

type HttpSchedulerClient struct {
	httpclient http.Client
}

func (c *HttpSchedulerClient) retireWorkers(cluster *daskv1alpha1.Cluster, n int) (RetireResult, error) {
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

	data := make(map[string]struct{ id string }, n)
	klog.V(2).Infof("scheduler response: %s", body)
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	return data, nil
}
