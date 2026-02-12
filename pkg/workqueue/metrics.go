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
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	QueueNameLabel = "queue_name"
	NamespaceLabel = "namespace"
	NameLabel      = "name"
)

// MetricsConfig contains the configuration for workqueue metrics.
// Metrics must be pre-registered by the caller and shared across queues.
type MetricsConfig struct {
	// QueueLength tracks the current queue length.
	QueueLength *prometheus.GaugeVec

	// QueueLatency tracks time items spend in the queue.
	QueueLatency *prometheus.HistogramVec

	// RequeueCount tracks the total number of times items have been requeued for retry.
	RequeueCount *prometheus.CounterVec
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
	requeueCount     *prometheus.CounterVec
	enqueueTimestamp sync.Map // map[string]time.Time
	queueName        string
	getLenFunc       func() int // function to get actual queue length
}

func newQueueMetrics(config *MetricsConfig, queueName string) *queueMetrics {
	if config == nil {
		return nil
	}

	return &queueMetrics{
		queueLength:  config.QueueLength,
		queueLatency: config.QueueLatency,
		requeueCount: config.RequeueCount,
		queueName:    queueName,
	}
}

func (m *queueMetrics) setLenFunc(f func() int) {
	if m != nil {
		m.getLenFunc = f
	}
}

func (m *queueMetrics) recordAdd(key string) {
	if m == nil {
		return
	}

	// Update gauge to actual queue length if available
	if m.queueLength != nil && m.getLenFunc != nil {
		m.queueLength.With(prometheus.Labels{QueueNameLabel: m.queueName}).Set(float64(m.getLenFunc()))
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

	// Update gauge to actual queue length if available
	if m.queueLength != nil && m.getLenFunc != nil {
		m.queueLength.With(prometheus.Labels{QueueNameLabel: m.queueName}).Set(float64(m.getLenFunc()))
	}

	if m.queueLatency != nil {
		if enqueueTime, ok := m.enqueueTimestamp.LoadAndDelete(key); ok {
			latency := float64(time.Since(enqueueTime.(time.Time)).Milliseconds())
			m.queueLatency.With(prometheus.Labels{QueueNameLabel: m.queueName}).Observe(latency)
		}
	}
}

func (m *queueMetrics) recordRequeue(key string) {
	if m == nil {
		return
	}

	if m.requeueCount != nil {
		ns, name := parseKey(key)
		m.requeueCount.With(prometheus.Labels{
			QueueNameLabel: m.queueName,
			NamespaceLabel: ns,
			NameLabel:      name,
		}).Inc()
	}
}

// parseKey parses a workqueue key in the format "namespace/name" or just "name".
func parseKey(key string) (namespace, name string) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}

	return "", key
}
