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
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type HAManager struct {
	leaderElection   ILeaderElection
	stateSynchronizer IStateSynchronizer
	healthChecker     IHealthChecker
	logger            *zap.SugaredLogger
	currentState      ControllerState
	mu                sync.RWMutex
	promotionChan     chan struct{}
	isStopped         bool
}

type HAManagerConfig struct {
	KubeClient      kubernetes.Interface
	CRTClient       client.Client
	Identity        string
	Namespace       string
	LeaseName       string
	Logger          *zap.SugaredLogger
	LeaseDuration   time.Duration
	RenewDeadline   time.Duration
	RetryPeriod     time.Duration
	SyncInterval    time.Duration
	HealthCheckInterval time.Duration
	FailureThreshold int
}

func NewHAManager(config HAManagerConfig) (*HAManager, error) {
	if config.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	if config.LeaseDuration <= 0 {
		config.LeaseDuration = 60 * time.Second
	}
	if config.RenewDeadline <= 0 {
		config.RenewDeadline = 40 * time.Second
	}
	if config.RetryPeriod <= 0 {
		config.RetryPeriod = 15 * time.Second
	}
	if config.SyncInterval <= 0 {
		config.SyncInterval = 30 * time.Second
	}
	if config.HealthCheckInterval <= 0 {
		config.HealthCheckInterval = 10 * time.Second
	}
	if config.FailureThreshold <= 0 {
		config.FailureThreshold = 3
	}

	leaderElection := NewLeaderElection(LeaderElectionConfig{
		KubeClient:    config.KubeClient,
		Identity:      config.Identity,
		Namespace:     config.Namespace,
		LeaseName:     config.LeaseName,
		LeaseDuration: config.LeaseDuration,
		RenewDeadline: config.RenewDeadline,
		RetryPeriod:   config.RetryPeriod,
		Logger:        config.Logger,
	})

	stateSynchronizer := NewStateSynchronizer(StateSynchronizerConfig{
		KubeClient:   config.CRTClient,
		Logger:       config.Logger,
		SyncInterval: config.SyncInterval,
	})

	healthChecker := NewHealthChecker(HealthCheckerConfig{
		KubeClient:       config.CRTClient,
		Logger:           config.Logger,
		Namespace:        config.Namespace,
		LeaseName:        config.LeaseName,
		CheckInterval:    config.HealthCheckInterval,
		FailureThreshold: config.FailureThreshold,
	})

	manager := &HAManager{
		leaderElection:    leaderElection,
		stateSynchronizer: stateSynchronizer,
		healthChecker:     healthChecker,
		logger:            config.Logger,
		promotionChan:     make(chan struct{}, 1),
		currentState: ControllerState{
			Role:           RoleStandby,
			IsLeader:       false,
			LeaderIdentity: "",
			LastHeartbeat:  time.Now(),
			LeaseDuration:  config.LeaseDuration,
			RenewDeadline:  time.Now().Add(config.RenewDeadline),
			TransitionTime: time.Now(),
		},
	}

	leaderElection.onAcquired = manager.onLeadershipAcquired
	leaderElection.onLost = manager.onLeadershipLost

	return manager, nil
}

func (hm *HAManager) Start(ctx context.Context) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if hm.isStopped {
		return fmt.Errorf("ha manager is already stopped")
	}

	hm.logger.Info("Starting HA Manager")

	if err := hm.leaderElection.Start(ctx); err != nil {
		return fmt.Errorf("failed to start leader election: %w", err)
	}

	if err := hm.stateSynchronizer.StartSync(ctx); err != nil {
		return fmt.Errorf("failed to start state synchronizer: %w", err)
	}

	if err := hm.healthChecker.StartHealthCheck(ctx); err != nil {
		return fmt.Errorf("failed to start health checker: %w", err)
	}

	hm.logger.Info("HA Manager started successfully")
	return nil
}

func (hm *HAManager) Stop() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.logger.Info("Stopping HA Manager")
	hm.leaderElection.Stop()
	hm.stateSynchronizer.StopSync()
	hm.healthChecker.StopHealthCheck()
	hm.isStopped = true
	hm.logger.Info("HA Manager stopped")
}

func (hm *HAManager) onLeadershipAcquired(ctx context.Context) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.currentState.Role = RoleActive
	hm.currentState.IsLeader = true
	hm.currentState.TransitionTime = time.Now()
	hm.logger.Infof("Transitioned to Active: %s", hm.currentState.TransitionTime)

	select {
	case hm.promotionChan <- struct{}{}:
	default:
	}
}

func (hm *HAManager) onLeadershipLost(ctx context.Context) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.currentState.Role = RoleStandby
	hm.currentState.IsLeader = false
	hm.currentState.TransitionTime = time.Now()
	hm.logger.Infof("Transitioned to Standby: %s", hm.currentState.TransitionTime)
}

func (hm *HAManager) GetCurrentState() ControllerState {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.currentState
}

func (hm *HAManager) IsActive() bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.currentState.Role == RoleActive
}

func (hm *HAManager) IsStandby() bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.currentState.Role == RoleStandby
}

func (hm *HAManager) WaitForActivePromotion(ctx context.Context) error {
	select {
	case <-hm.promotionChan:
		hm.logger.Info("Controller promoted to Active")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
