package main

import (
	"context"
	"testing"

	"github.com/CHARM-Tx/dask-operator/pkg/generated/clientset/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2/ktesting"
)

type fixture struct {
	t *testing.T
}

func newFixture(t *testing.T) *fixture {
	return &fixture{t: t}
}

func (f *fixture) newController(ctx context.Context) *Controller {
	kubeclient := k8sfake.NewSimpleClientset()
	client := fake.NewSimpleClientset()
	return NewController(kubeclient, client, ctx)
}

func TestSyncHandler(t *testing.T) {
	f := newFixture(t)
	_, ctx := ktesting.NewTestContext(t)
	key := ""

	controller := f.newController(ctx)
	controller.syncHandler(ctx, key)
}
