//go:generate deepcopy-gen --go-header-file=/dev/null --input-dirs=github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1 --trim-path-prefix=github.com/CHARM-Tx/dask-operator --output-base=.
//go:generate lister-gen --go-header-file=/dev/null --input-dirs=github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1 --output-package=github.com/CHARM-Tx/dask-operator/pkg/generated/listers --trim-path-prefix=github.com/CHARM-Tx/dask-operator --output-base=.
//go:generate applyconfiguration-gen --go-header-file=/dev/null --input-dirs=github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1 --output-package=github.com/CHARM-Tx/dask-operator/pkg/generated/applyconfigurations --trim-path-prefix=github.com/CHARM-Tx/dask-operator --output-base=.
//go:generate client-gen --go-header-file=/dev/null --clientset-name=clientset --apply-configuration-package=github.com/CHARM-Tx/dask-operator/pkg/generated/applyconfigurations --input-base=github.com/CHARM-Tx/dask-operator/pkg/apis --input=dask/v1alpha1 --output-package=github.com/CHARM-Tx/dask-operator/pkg/generated --trim-path-prefix github.com/CHARM-Tx/dask-operator --output-base=.
//go:generate informer-gen --go-header-file=/dev/null --versioned-clientset-package=github.com/CHARM-Tx/dask-operator/pkg/generated/clientset --listers-package=github.com/CHARM-Tx/dask-operator/pkg/generated/listers --input-dirs=github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1 --output-package=github.com/CHARM-Tx/dask-operator/pkg/generated/informers --trim-path-prefix github.com/CHARM-Tx/dask-operator --output-base=.
//go:generate controller-gen paths=./... crd:maxDescLen=0

package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/CHARM-Tx/dask-operator/pkg/generated/clientset"
	flag "github.com/spf13/pflag"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

func init() {
	klog.InitFlags(nil)
}

func signalContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cancel()

		// If the signal is received a second time, exit immediately
		<-c
		os.Exit(1)
	}()

	return ctx
}

func main() {
	flag.Parse()

	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	kubeclient, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	client, err := clientset.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	schedulerClient := &HttpSchedulerClient{httpclient: http.Client{}}

	ctx := signalContext()
	controller := NewController(kubeclient, client, schedulerClient, ctx)
	controller.Run(1, context.Background())
}
