// Copyright 2018 The Prometheus Authors
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

package main

import (
	"context"
	"fmt"
	"github.com/seanly/prometheus-ecs-sd/pkg/aliyun"
	"github.com/seanly/prometheus-ecs-sd/pkg/config"
	"github.com/seanly/prometheus-ecs-sd/pkg/util"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"

	"github.com/prometheus/prometheus/discovery/targetgroup"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/util/strutil"
)

var (
	a            = kingpin.New("sd adapter usage", "Tool to generate Prometheus file_sd target files for Alicloud ECS.")
	outputf      = a.Flag("output.file", "The output filename for file_sd compatible file.").Default("ecs.json").String()
	configf      = a.Flag("config.file", "The config filename for ecs_sd config file.").Default("config/ecs_sd_config.yml").String()
	listen       = a.Flag("web.listen-address", "The listen address.").Default(":9465").String()

	ecsPrefix = model.MetaLabelPrefix + "ecs_"
	// nodeLabel is the name for the label containing the server's name.
	ecsLabelInstanceName = ecsPrefix + "instance_name"
	ecsLabelInstanceId = ecsPrefix + "instance_id"
	ecsLabelInstanceType = ecsPrefix + "instance_type"
	// privateIPLabel is the name for the label containing the server's private IP.
	ecsLabelPrivateIP = ecsPrefix + "private_ip"
	// publicIPLabel is the name for the label containing the server's public IP.
	ecsLabelPublicIP = ecsPrefix + "public_ip"
	// stateLabel is the name for the label containing the server's state.
	ecsLabelStatus = ecsPrefix + "status"
	// tagsLabel is the name for the label containing all the server's tags.
	ecsLabelTag = ecsPrefix + "tag_"
	// zoneLabel is the name for the label containing all the server's zone location.
	ecsLabelRegionId = ecsPrefix + "region_id"
	ecsLabelVPCID = ecsPrefix + "vpc_id"
)

var (
	reg             = prometheus.NewRegistry()
	requestDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "prometheus_ecs_sd_request_duration_seconds",
			Help:    "Histogram of latencies for requests to the Alicloud ECS API.",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0},
		},
	)
	requestFailures = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_ecs_sd_request_failures_total",
			Help: "Total number of failed requests to the Alicloud ECS API.",
		},
	)
)

func init() {
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{
		PidFn: func() (i int, e error) {
			return os.Getpid(), nil
		},
		Namespace:    "",
		ReportErrors: false,
	}))
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(version.NewCollector("prometheus_ecs_sd"))
	reg.MustRegister(requestDuration)
	reg.MustRegister(requestFailures)
}

// ecsDiscoverer retrieves target information from the aliyun API.
type ecsDiscoverer struct {
	ecsSDConfig *config.SDConfig
	port        int
	refresh     model.Duration
	lasts       map[string]struct{}
	logger      log.Logger
}

func (d *ecsDiscoverer) createTarget(srv *ecs.Instance) *targetgroup.Group {

	tg := &targetgroup.Group{
		Source: fmt.Sprintf("ecs/%s", srv.InstanceId),
	}

	privateIp := srv.VpcAttributes.PrivateIpAddress.IpAddress
	if len(privateIp) == 0 {
		return tg
	}

	addr := net.JoinHostPort(privateIp[0], fmt.Sprintf("%d", d.port))
	tg.Targets = []model.LabelSet {
		{
			model.AddressLabel: model.LabelValue(addr),
		},
	}

	labels := model.LabelSet{}

	labels[model.LabelName(ecsLabelInstanceId)] = model.LabelValue(srv.InstanceId)
	labels[model.LabelName(ecsLabelInstanceName)] = model.LabelValue(srv.InstanceName)
	labels[model.LabelName(ecsLabelInstanceType)] = model.LabelValue(srv.InstanceType)
	labels[model.LabelName(ecsLabelPrivateIP)] = model.LabelValue(privateIp[0])
	labels[model.LabelName(ecsLabelStatus)] = model.LabelValue(srv.Status)
	labels[model.LabelName(ecsLabelRegionId)] = model.LabelValue(srv.RegionId)
	labels[model.LabelName(ecsLabelVPCID)] = model.LabelValue(srv.VpcAttributes.VpcId)

	if len(srv.PublicIpAddress.IpAddress) > 0 {
		labels[model.LabelName(ecsLabelPublicIP)] = model.LabelValue(srv.PublicIpAddress.IpAddress[0])
	}

	for _, t:= range srv.Tags.Tag {
		if t.TagKey == "" || t.TagValue == "" {
			continue
		}
		name := strutil.SanitizeLabelName(t.TagKey)
		labels[model.LabelName(ecsLabelTag+name)] = model.LabelValue(t.TagValue)
	}

	tg.Labels = labels

	return tg
}

func (d *ecsDiscoverer) getTargets() ([]*targetgroup.Group, error) {
	now := time.Now()
	ecsClient := aliyun.NewEcsClient(d.ecsSDConfig)
	srvs, err := ecsClient.GetServers()
	requestDuration.Observe(time.Since(now).Seconds())
	if err != nil {
		requestFailures.Inc()
		return nil, err
	}

	_ = level.Debug(d.logger).Log("msg", "get servers", "nb", len(*srvs))

	current := make(map[string]struct{})
	tgs := make([]*targetgroup.Group, len(*srvs))
	for _, s := range *srvs {
		tg := d.createTarget(&s)
		_ = level.Debug(d.logger).Log("msg", "server added", "source", tg.Source)
		current[tg.Source] = struct{}{}
		tgs = append(tgs, tg)
	}

	// Add empty groups for servers which have been removed since the last refresh.
	for k := range d.lasts {
		if _, ok := current[k]; !ok {
			_ = level.Debug(d.logger).Log("msg", "server deleted", "source", k)
			tgs = append(tgs, &targetgroup.Group{Source: k})
		}
	}
	d.lasts = current

	return tgs, nil
}

func (d *ecsDiscoverer) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	for c := time.Tick(time.Duration(d.refresh)); ; {
		tgs, err := d.getTargets()
		if err == nil {
			ch <- tgs
		}

		// Wait for ticker or exit when ctx is closed.
		select {
		case <-c:
			continue
		case <-ctx.Done():
			return
		}
	}
}


func main() {

	a.HelpFlag.Short('h')
	a.Version(version.Print("prometheus-ecs-sd"))

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	logger := &util.EcsLogger{
		log.With(
			log.NewSyncLogger(log.NewLogfmtLogger(os.Stdout)),
			"ts", log.DefaultTimestampUTC,
			"caller", log.DefaultCaller,
		),
	}

	conf, err := config.LoadFile(*configf)
	if err != nil {
		_ = errors.Wrapf(err, "couldn't load configuration (--config.file=%q)", *configf)
		return
	}

	ctx := context.Background()
	disc := &ecsDiscoverer{
		ecsSDConfig: &conf.EcsSDConfig,
		port:      conf.EcsSDConfig.Port,
		refresh:   conf.EcsSDConfig.RefreshInterval,
		logger:    logger,
		lasts:     make(map[string]struct{}),
	}
	sdAdapter := util.NewAdapter(ctx, *outputf, "ecsSD", disc, logger)
	sdAdapter.Run()

	_ = level.Debug(logger).Log("msg", "listening for connections", "addr", *listen)
	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{ErrorLog: logger}))
	if err := http.ListenAndServe(*listen, nil); err != nil {
		_ = level.Debug(logger).Log("msg", "failed to listen", "addr", *listen, "err", err)
		os.Exit(1)
	}
}
