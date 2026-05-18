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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type StateSynchronizer struct {
	kubeClient   client.Client
	logger       *zap.SugaredLogger
	syncInterval time.Duration
	isSynced     bool
	lastSyncTime map[string]time.Time
	mu           sync.RWMutex
	ticker       *time.Ticker
	stopChan     chan struct{}
}

type StateSynchronizerConfig struct {
	KubeClient   client.Client
	Logger       *zap.SugaredLogger
	SyncInterval time.Duration
}

func NewStateSynchronizer(config StateSynchronizerConfig) *StateSynchronizer {
	return &StateSynchronizer{
		kubeClient:   config.KubeClient,
		logger:       config.Logger,
		syncInterval: config.SyncInterval,
		isSynced:     false,
		lastSyncTime: make(map[string]time.Time),
		stopChan:     make(chan struct{}),
	}
}

func (ss *StateSynchronizer) StartSync(ctx context.Context) error {
	if ss.syncInterval <= 0 {
		ss.syncInterval = 30 * time.Second
	}

	ss.ticker = time.NewTicker(ss.syncInterval)
	ss.logger.Infof("State synchronizer started with interval: %v", ss.syncInterval)

	go func() {
		for {
			select {
			case <-ss.stopChan:
				ss.logger.Info("State synchronizer stopped")
				return
			case <-ss.ticker.C:
				ss.performSync(ctx)
			}
		}
	}()

	return nil
}

func (ss *StateSynchronizer) StopSync() {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if ss.ticker != nil {
		ss.ticker.Stop()
	}
	close(ss.stopChan)
}

func (ss *StateSynchronizer) performSync(ctx context.Context) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.logger.Debug("Performing state synchronization")
	ss.isSynced = true
	ss.logger.Debug("State synchronization completed")
}

func (ss *StateSynchronizer) SyncResource(ctx context.Context, resourceType string, resourceName string, namespace string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.logger.Debugf("Syncing resource: type=%s, name=%s, namespace=%s", resourceType, resourceName, namespace)

	ss.lastSyncTime[fmt.Sprintf("%s/%s/%s", resourceType, namespace, resourceName)] = time.Now()
	return nil
}

func (ss *StateSynchronizer) GetLastSyncTime(resourceType string) time.Time {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	if t, exists := ss.lastSyncTime[resourceType]; exists {
		return t
	}
	return time.Time{}
}

func (ss *StateSynchronizer) IsSynced() bool {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.isSynced
}

func (ss *StateSynchronizer) WaitForSync(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		if ss.IsSynced() {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("state synchronization timeout after %v", timeout)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
