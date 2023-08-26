//go:generate deepcopy-gen --go-header-file=/dev/null --input-dirs=github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1 --trim-path-prefix=github.com/CHARM-Tx/dask-operator --output-base=.
//go:generate lister-gen --go-header-file=/dev/null --input-dirs=github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1 --output-package=github.com/CHARM-Tx/dask-operator/pkg/generated/listers --trim-path-prefix=github.com/CHARM-Tx/dask-operator --output-base=.
//go:generate client-gen --go-header-file=/dev/null --clientset-name=clientset --input-base=github.com/CHARM-Tx/dask-operator/pkg/apis --input=dask/v1alpha1 --output-package=github.com/CHARM-Tx/dask-operator/pkg/generated --trim-path-prefix github.com/CHARM-Tx/dask-operator --output-base=.
//go:generate informer-gen --go-header-file=/dev/null --versioned-clientset-package=github.com/CHARM-Tx/dask-operator/pkg/generated/clientset --listers-package=github.com/CHARM-Tx/dask-operator/pkg/generated/listers --input-dirs=github.com/CHARM-Tx/dask-operator/pkg/apis/dask/v1alpha1 --output-package=github.com/CHARM-Tx/dask-operator/pkg/generated/informers --trim-path-prefix github.com/CHARM-Tx/dask-operator --output-base=.

package main

import (
	"context"

	flag "github.com/spf13/pflag"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

func init() {
	klog.InitFlags(nil)
}

func main() {
	flag.Parse()

	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	controller := NewController(clientset)
	controller.Run(1, context.Background())
}
