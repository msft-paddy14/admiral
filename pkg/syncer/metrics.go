/*
SPDX-License-Identifier: Apache-2.0

Copyright Contributors to the Submariner project.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package syncer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	ResourceTypeLabel = "resource_type"
)

// LatencyMetricsConfig contains the configuration for latency metrics.
type LatencyMetricsConfig struct {
	// TransformLatencyOpts if specified, used to create a histogram to record transform latency metrics.
	// Transform latency measures the time taken to transform a resource.
	TransformLatencyOpts *prometheus.HistogramOpts

	// FederationLatencyOpts if specified, used to create a histogram to record federation latency metrics.
	// Federation latency measures the time taken to distribute or delete a resource via the federator.
	FederationLatencyOpts *prometheus.HistogramOpts
}

// latencyMetrics holds the prometheus metrics for measuring latencies.
type latencyMetrics struct {
	transformLatency  *prometheus.HistogramVec
	federationLatency *prometheus.HistogramVec
	direction         SyncDirection
	syncerName        string
}

// DefaultLatencyBuckets provides default bucket boundaries for latency histograms.
// These buckets range from 1ms to 60000ms (60s), covering typical Kubernetes operation latencies.
var DefaultLatencyBuckets = []float64{
	1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000,
}

func newLatencyMetrics(config LatencyMetricsConfig, direction SyncDirection, syncerName string) *latencyMetrics {
	m := &latencyMetrics{
		direction:  direction,
		syncerName: syncerName,
	}

	labels := []string{
		DirectionLabel,
		OperationLabel,
		SyncerNameLabel,
	}

	if config.TransformLatencyOpts != nil {
		opts := *config.TransformLatencyOpts
		if opts.Buckets == nil {
			opts.Buckets = DefaultLatencyBuckets
		}

		m.transformLatency = prometheus.NewHistogramVec(opts, labels)
		prometheus.MustRegister(m.transformLatency)
	}

	if config.FederationLatencyOpts != nil {
		opts := *config.FederationLatencyOpts
		if opts.Buckets == nil {
			opts.Buckets = DefaultLatencyBuckets
		}

		m.federationLatency = prometheus.NewHistogramVec(opts, labels)
		prometheus.MustRegister(m.federationLatency)
	}

	return m
}

func (m *latencyMetrics) unregister() {
	if m == nil {
		return
	}

	if m.transformLatency != nil {
		prometheus.Unregister(m.transformLatency)
	}

	if m.federationLatency != nil {
		prometheus.Unregister(m.federationLatency)
	}
}

func (m *latencyMetrics) recordTransformLatency(startTime time.Time, direction SyncDirection, op Operation, syncerName string) {
	if m == nil || m.transformLatency == nil {
		return
	}

	latency := float64(time.Since(startTime).Milliseconds())
	m.transformLatency.With(prometheus.Labels{
		DirectionLabel:  direction.String(),
		OperationLabel:  op.String(),
		SyncerNameLabel: syncerName,
	}).Observe(latency)
}

func (m *latencyMetrics) recordFederationLatency(startTime time.Time, direction SyncDirection, op Operation, syncerName string) {
	if m == nil || m.federationLatency == nil {
		return
	}

	latency := float64(time.Since(startTime).Milliseconds())
	m.federationLatency.With(prometheus.Labels{
		DirectionLabel:  direction.String(),
		OperationLabel:  op.String(),
		SyncerNameLabel: syncerName,
	}).Observe(latency)
}
