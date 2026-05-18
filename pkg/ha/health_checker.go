/*
 * 	Copyright (c) 2022 Avesha, Inc. All rights reserved. # # SPDX-License-Identifier: Apache-2.0
 *
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 */

package ha

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type HealthChecker struct {
	kubeClient      client.Client
	logger          *zap.SugaredLogger
	namespace       string
	leaseName       string
	checkInterval   time.Duration
	failureThreshold int
	status          HealthStatus
	mu              sync.RWMutex
	ticker          *time.Ticker
	stopChan        chan struct{}
}

type HealthCheckerConfig struct {
	KubeClient       client.Client
	Logger           *zap.SugaredLogger
	Namespace        string
	LeaseName        string
	CheckInterval    time.Duration
	FailureThreshold int
}

func NewHealthChecker(config HealthCheckerConfig) *HealthChecker {
	if config.CheckInterval <= 0 {
		config.CheckInterval = 10 * time.Second
	}
	if config.FailureThreshold <= 0 {
		config.FailureThreshold = 3
	}

	return &HealthChecker{
		kubeClient:       config.KubeClient,
		logger:           config.Logger,
		namespace:        config.Namespace,
		leaseName:        config.LeaseName,
		checkInterval:    config.CheckInterval,
		failureThreshold: config.FailureThreshold,
		status: HealthStatus{
			IsHealthy:           true,
			LastCheck:           time.Now(),
			ConsecutiveFailures: 0,
			LastError:           nil,
		},
		stopChan: make(chan struct{}),
	}
}

func (hc *HealthChecker) StartHealthCheck(ctx context.Context) error {
	hc.ticker = time.NewTicker(hc.checkInterval)
	hc.logger.Infof("Health checker started with interval: %v", hc.checkInterval)

	go func() {
		for {
			select {
			case <-hc.stopChan:
				hc.logger.Info("Health checker stopped")
				return
			case <-hc.ticker.C:
				hc.performHealthCheck(ctx)
			}
		}
	}()

	return nil
}

func (hc *HealthChecker) StopHealthCheck() {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if hc.ticker != nil {
		hc.ticker.Stop()
	}
	close(hc.stopChan)
}

func (hc *HealthChecker) performHealthCheck(ctx context.Context) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if hc.kubeClient == nil {
		return
	}

	lease := &coordinationv1.Lease{}
	err := hc.kubeClient.Get(ctx, types.NamespacedName{
		Name:      hc.leaseName,
		Namespace: hc.namespace,
	}, lease)

	if err != nil {
		hc.logger.Warnf("Health check failed: %v", err)
		hc.status.ConsecutiveFailures++
		hc.status.LastError = err

		if hc.status.ConsecutiveFailures >= hc.failureThreshold {
			hc.status.IsHealthy = false
			hc.logger.Warnf("Controller marked unhealthy after %d failures", hc.failureThreshold)
		}
	} else {
		if lease.Spec.HolderIdentity != nil && len(*lease.Spec.HolderIdentity) > 0 {
			if lease.Spec.RenewTime != nil {
				timeSinceLastRenewal := time.Since(lease.Spec.RenewTime.Time)
				if timeSinceLastRenewal > time.Duration(*lease.Spec.LeaseDurationSeconds)*time.Second {
					hc.logger.Warnf("Leader lease stale: not renewed for %v", timeSinceLastRenewal)
					hc.status.ConsecutiveFailures++
				} else {
					hc.status.ConsecutiveFailures = 0
					if !hc.status.IsHealthy {
						hc.status.IsHealthy = true
						hc.logger.Info("Controller recovered and marked healthy")
					}
				}
			}
		}
		hc.status.LastError = nil
	}

	hc.status.LastCheck = time.Now()
	hc.logger.Debugf("Health check completed: healthy=%v, failures=%d", hc.status.IsHealthy, hc.status.ConsecutiveFailures)
}

func (hc *HealthChecker) IsHealthy() bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.status.IsHealthy
}

func (hc *HealthChecker) GetHealthStatus() HealthStatus {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.status
}

func (hc *HealthChecker) WaitForHealthy(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		if hc.IsHealthy() {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("health check timeout: controller not healthy after %v", timeout)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
