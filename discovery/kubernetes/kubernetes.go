// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kubernetes

import (
	"io/ioutil"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api"
	apiv1 "k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"golang.org/x/net/context"
)

const (
	// kubernetesMetaLabelPrefix is the meta prefix used for all meta labels.
	// in this discovery.
	metaLabelPrefix = model.MetaLabelPrefix + "kubernetes_"
	namespaceLabel  = metaLabelPrefix + "namespace"
)

var (
	eventCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_kubernetes_events_total",
			Help: "The number of Kubernetes events handled.",
		},
		[]string{"role", "event"},
	)
)

func init() {
	prometheus.MustRegister(eventCount)

	// Initialize metric vectors.
	for _, role := range []string{"endpoints", "node", "pod", "service"} {
		for _, evt := range []string{"add", "delete", "update"} {
			eventCount.WithLabelValues(role, evt)
		}
	}
}

// Discovery implements the TargetProvider interface for discovering
// targets from Kubernetes.
type Discovery struct {
	url                string
	client             kubernetes.Interface
	role               config.KubernetesRole
	logger             log.Logger
	namespaceDiscovery *config.KubernetesNamespaceDiscovery
}

func init() {
	utilruntime.ErrorHandlers = []func(error){
		func(err error) {
			log.With("component", "kube_client_runtime").Errorln(err)
		},
	}
}

func (d *Discovery) getNamespaces() []string {
	namespaces := d.namespaceDiscovery.Names
	if len(namespaces) == 0 {
		namespaces = []string{api.NamespaceAll}
	}
	return namespaces
}

// New creates a new Kubernetes discovery for the given role.
func New(l log.Logger, conf *config.KubernetesSDConfig) (*Discovery, error) {
	var (
		kcfg *rest.Config
		err  error
	)

	if conf.APIServer.URL == nil {
		// Use the Kubernetes provided pod service account
		// as described in https://kubernetes.io/docs/admin/service-accounts-admin/
		kcfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
		// Because the handling of configuration parameters changes
		// we should inform the user when their currently configured values
		// will be ignored due to precedence of InClusterConfig
		l.Info("Using pod service account via in-cluster config")
		if conf.TLSConfig.CAFile != "" {
			l.Warn("Configured TLS CA file is ignored when using pod service account")
		}
		if conf.TLSConfig.CertFile != "" || conf.TLSConfig.KeyFile != "" {
			l.Warn("Configured TLS client certificate is ignored when using pod service account")
		}
		if conf.BearerToken != "" {
			l.Warn("Configured auth token is ignored when using pod service account")
		}
		if conf.BasicAuth != nil {
			l.Warn("Configured basic authentication credentials are ignored when using pod service account")
		}
	} else {
		kcfg = &rest.Config{
			Host: conf.APIServer.String(),
			TLSClientConfig: rest.TLSClientConfig{
				CAFile:   conf.TLSConfig.CAFile,
				CertFile: conf.TLSConfig.CertFile,
				KeyFile:  conf.TLSConfig.KeyFile,
				Insecure: conf.TLSConfig.InsecureSkipVerify,
			},
		}
		token := conf.BearerToken
		if conf.BearerTokenFile != "" {
			bf, err := ioutil.ReadFile(conf.BearerTokenFile)
			if err != nil {
				return nil, err
			}
			token = string(bf)
		}
		kcfg.BearerToken = token

		if conf.BasicAuth != nil {
			kcfg.Username = conf.BasicAuth.Username
			kcfg.Password = conf.BasicAuth.Password
		}
	}

	kcfg.UserAgent = "prometheus/discovery"

	c, err := kubernetes.NewForConfig(kcfg)
	if err != nil {
		return nil, err
	}
	d := &Discovery{
		client:             c,
		logger:             l,
		role:               conf.Role,
		namespaceDiscovery: &conf.NamespaceDiscovery,
	}
	if conf.APIServer.URL != nil {
		d.url = conf.APIServer.URL.String()
	}

	return d, nil
}

const resyncPeriod = 10 * time.Minute

// informapMap holds informers against different clusters and namespaces
// for deduplication
type informerMap struct {
	cancel func()
	sync.Mutex
	m map[string]map[string]*informerEntry
}

type informerEntry struct {
	cache.SharedInformer
	cancel func()
	refs   int
}

func (m informerMap) done(u string, ns, kind string) {
	infs := m.m[u]
	if infs == nil {
		return
	}
	key := ns + "/" + kind
	e, ok := infs[key]
	if !ok {
		return
	}
	e.refs--
	if e.refs > 0 {
		return
	}
	e.cancel()
	delete(infs, key)
}

func (m informerMap) get(
	u string,
	kclient kubernetes.Interface,
	ns string,
	kind string,
) cache.SharedInformer {
	m.Lock()
	defer m.Unlock()

	mapping := map[string]runtime.Object{
		"endpoints": &apiv1.Endpoints{},
		"services":  &apiv1.Service{},
		"pods":      &apiv1.Pod{},
		"nodes":     &apiv1.Node{},
	}

	rclient := kclient.Core().RESTClient()
	key := ns + "/" + kind

	infs, ok := m.m[u]
	if !ok {
		infs = map[string]*informerEntry{}
		m.m[u] = infs
	}
	f, ok := infs[key]
	if !ok {
		ctx, cancel := context.WithCancel(context.Background())

		lw := cache.NewListWatchFromClient(rclient, kind, ns, nil)
		f = &informerEntry{
			SharedInformer: cache.NewSharedInformer(lw, mapping[kind], resyncPeriod),
			cancel:         cancel,
		}
		go f.Run(ctx.Done())

		for !f.HasSynced() {
			time.Sleep(100 * time.Millisecond)
		}
		infs[key] = f
	}
	f.refs++

	return f
}

var infmap = informerMap{
	m: map[string]map[string]*informerEntry{},
}

// Run implements the TargetProvider interface.
func (d *Discovery) Run(ctx context.Context, ch chan<- []*config.TargetGroup) {
	namespaces := d.getNamespaces()

	switch d.role {
	case "endpoints":
		var wg sync.WaitGroup

		for _, namespace := range namespaces {
			eps := NewEndpoints(
				d.logger.With("kubernetes_sd", "endpoint"),
				infmap.get(d.url, d.client, namespace, "services"),
				infmap.get(d.url, d.client, namespace, "endpoints"),
				infmap.get(d.url, d.client, namespace, "pods"),
			)

			wg.Add(1)
			go func(ns string) {
				defer wg.Done()
				eps.Run(ctx, ch)
				infmap.done(d.url, ns, "endpoints")
				infmap.done(d.url, ns, "services")
				infmap.done(d.url, ns, "pods")
			}(namespace)
		}
		wg.Wait()
	case "pod":
		var wg sync.WaitGroup
		for _, namespace := range namespaces {
			pod := NewPod(
				d.logger.With("kubernetes_sd", "pod"),
				infmap.get(d.url, d.client, namespace, "pods"),
			)
			wg.Add(1)
			go func(ns string) {
				defer wg.Done()
				pod.Run(ctx, ch)
				infmap.done(d.url, ns, "pods")
			}(namespace)
		}
		wg.Wait()
	case "service":
		var wg sync.WaitGroup
		for _, namespace := range namespaces {
			svc := NewService(
				d.logger.With("kubernetes_sd", "service"),
				infmap.get(d.url, d.client, namespace, "service"),
			)
			wg.Add(1)
			go func(ns string) {
				defer wg.Done()
				svc.Run(ctx, ch)
				infmap.done(d.url, ns, "service")
			}(namespace)
		}
		wg.Wait()
	case "node":
		node := NewNode(
			d.logger.With("kubernetes_sd", "pod"),
			infmap.get(d.url, d.client, api.NamespaceAll, "nodes"),
		)
		node.Run(ctx, ch)
		infmap.done(d.url, api.NamespaceAll, "nodes")

	default:
		d.logger.Errorf("unknown Kubernetes discovery kind %q", d.role)
	}

	<-ctx.Done()
}

func lv(s string) model.LabelValue {
	return model.LabelValue(s)
}
