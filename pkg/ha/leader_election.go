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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type LeaderElection struct {
	kubeClient       kubernetes.Interface
	identity         string
	namespace        string
	leaseName        string
	leaseDuration    time.Duration
	renewDeadline    time.Duration
	retryPeriod      time.Duration
	logger           *zap.SugaredLogger
	isLeader         bool
	leaderIdentity   string
	mu               sync.RWMutex
	leaderElector    *leaderelection.LeaderElector
	onAcquired       func(ctx context.Context)
	onLost           func(ctx context.Context)
}

type LeaderElectionConfig struct {
	KubeClient    kubernetes.Interface
	Identity      string
	Namespace     string
	LeaseName     string
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
	Logger        *zap.SugaredLogger
	OnAcquired    func(ctx context.Context)
	OnLost        func(ctx context.Context)
}

func NewLeaderElection(config LeaderElectionConfig) *LeaderElection {
	return &LeaderElection{
		kubeClient:    config.KubeClient,
		identity:      config.Identity,
		namespace:     config.Namespace,
		leaseName:     config.LeaseName,
		leaseDuration: config.LeaseDuration,
		renewDeadline: config.RenewDeadline,
		retryPeriod:   config.RetryPeriod,
		logger:        config.Logger,
		isLeader:      false,
		onAcquired:    config.OnAcquired,
		onLost:        config.OnLost,
	}
}

func (le *LeaderElection) Start(ctx context.Context) error {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      le.leaseName,
			Namespace: le.namespace,
		},
		Client: le.kubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: le.identity,
		},
	}

	config := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   le.leaseDuration,
		RenewDeadline:   le.renewDeadline,
		RetryPeriod:     le.retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				le.mu.Lock()
				le.isLeader = true
				le.mu.Unlock()
				le.logger.Infof("Controller acquired leadership: %s", le.identity)
				if le.onAcquired != nil {
					le.onAcquired(ctx)
				}
			},
			OnStoppedLeading: func() {
				le.mu.Lock()
				le.isLeader = false
				le.mu.Unlock()
				le.logger.Infof("Controller lost leadership: %s", le.identity)
				if le.onLost != nil {
					le.onLost(context.Background())
				}
			},
			OnNewLeader: func(identity string) {
				if identity != le.identity {
					le.mu.Lock()
					le.leaderIdentity = identity
					le.mu.Unlock()
					le.logger.Infof("New leader elected: %s", identity)
				}
			},
		},
	}

	elector, err := leaderelection.NewLeaderElector(config)
	if err != nil {
		return fmt.Errorf("failed to create leader elector: %w", err)
	}

	le.leaderElector = elector
	go elector.Run(ctx)
	return nil
}

func (le *LeaderElection) Stop() {
	le.mu.Lock()
	defer le.mu.Unlock()
	if le.leaderElector != nil {
		le.logger.Info("Stopping leader election")
	}
}

func (le *LeaderElection) IsLeader() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.isLeader
}

func (le *LeaderElection) GetLeaderIdentity() string {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leaderIdentity
}

func (le *LeaderElection) OnLeadershipAcquired(ctx context.Context) {
	le.mu.Lock()
	defer le.mu.Unlock()
	le.isLeader = true
}

func (le *LeaderElection) OnLeadershipLost(ctx context.Context) {
	le.mu.Lock()
	defer le.mu.Unlock()
	le.isLeader = false
}
