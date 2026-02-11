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

package workqueue

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	QueueNameLabel = "queue_name"
)

// MetricsConfig contains the configuration for workqueue metrics.
// Metrics must be pre-registered by the caller and shared across queues.
type MetricsConfig struct {
	// QueueLength tracks the current queue length.
	QueueLength *prometheus.GaugeVec

	// QueueLatency tracks time items spend in the queue.
	QueueLatency *prometheus.HistogramVec
}

// DefaultLatencyBuckets provides default bucket boundaries for latency histograms.
// These buckets range from 1ms to 60000ms (60s), covering typical queue latencies.
var DefaultLatencyBuckets = []float64{
	1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000,
}

// queueMetrics holds prometheus metrics for a workqueue.
type queueMetrics struct {
	queueLength      *prometheus.GaugeVec
	queueLatency     *prometheus.HistogramVec
	enqueueTimestamp sync.Map // map[string]time.Time
	queueName        string
}

func newQueueMetrics(config *MetricsConfig, queueName string) *queueMetrics {
	if config == nil {
		return nil
	}

	return &queueMetrics{
		queueLength:  config.QueueLength,
		queueLatency: config.QueueLatency,
		queueName:    queueName,
	}
}

func (m *queueMetrics) recordAdd(key string) {
	if m == nil {
		return
	}

	if m.queueLength != nil {
		m.queueLength.With(prometheus.Labels{QueueNameLabel: m.queueName}).Inc()
	}

	if m.queueLatency != nil {
		// Use LoadOrStore to preserve the original timestamp if the key is re-enqueued
		// while still in the queue (workqueue coalesces duplicate keys).
		m.enqueueTimestamp.LoadOrStore(key, time.Now())
	}
}

func (m *queueMetrics) recordGet(key string) {
	if m == nil {
		return
	}

	if m.queueLength != nil {
		m.queueLength.With(prometheus.Labels{QueueNameLabel: m.queueName}).Dec()
	}

	if m.queueLatency != nil {
		if enqueueTime, ok := m.enqueueTimestamp.LoadAndDelete(key); ok {
			latency := float64(time.Since(enqueueTime.(time.Time)).Milliseconds())
			m.queueLatency.With(prometheus.Labels{QueueNameLabel: m.queueName}).Observe(latency)
		}
	}
}
