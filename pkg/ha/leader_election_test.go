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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLeaderElection_NewLeaderElection(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := LeaderElectionConfig{
		KubeClient:    nil,
		Identity:      "test-controller-1",
		Namespace:     "kubeslice-controller",
		LeaseName:     "kubeslice-controller-lease",
		LeaseDuration: 60 * time.Second,
		RenewDeadline: 40 * time.Second,
		RetryPeriod:   15 * time.Second,
		Logger:        sugaredLogger,
	}

	le := NewLeaderElection(config)

	require.NotNil(t, le)
	require.Equal(t, "test-controller-1", le.identity)
	require.Equal(t, "kubeslice-controller", le.namespace)
	require.Equal(t, "kubeslice-controller-lease", le.leaseName)
	require.False(t, le.isLeader)
	require.Equal(t, "", le.leaderIdentity)
}

func TestLeaderElection_IsLeader_InitiallyFalse(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := LeaderElectionConfig{
		KubeClient:    nil,
		Identity:      "test-controller-1",
		Namespace:     "kubeslice-controller",
		LeaseName:     "kubeslice-controller-lease",
		LeaseDuration: 60 * time.Second,
		RenewDeadline: 40 * time.Second,
		RetryPeriod:   15 * time.Second,
		Logger:        sugaredLogger,
	}

	le := NewLeaderElection(config)
	require.False(t, le.IsLeader())
}

func TestLeaderElection_GetLeaderIdentity_InitiallyEmpty(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := LeaderElectionConfig{
		KubeClient:    nil,
		Identity:      "test-controller-1",
		Namespace:     "kubeslice-controller",
		LeaseName:     "kubeslice-controller-lease",
		LeaseDuration: 60 * time.Second,
		RenewDeadline: 40 * time.Second,
		RetryPeriod:   15 * time.Second,
		Logger:        sugaredLogger,
	}

	le := NewLeaderElection(config)
	require.Equal(t, "", le.GetLeaderIdentity())
}

func TestLeaderElection_OnLeadershipAcquired(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := LeaderElectionConfig{
		KubeClient:    nil,
		Identity:      "test-controller-1",
		Namespace:     "kubeslice-controller",
		LeaseName:     "kubeslice-controller-lease",
		LeaseDuration: 60 * time.Second,
		RenewDeadline: 40 * time.Second,
		RetryPeriod:   15 * time.Second,
		Logger:        sugaredLogger,
	}

	le := NewLeaderElection(config)
	ctx := context.Background()

	le.OnLeadershipAcquired(ctx)
	require.True(t, le.IsLeader())
}

func TestLeaderElection_OnLeadershipLost(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := LeaderElectionConfig{
		KubeClient:    nil,
		Identity:      "test-controller-1",
		Namespace:     "kubeslice-controller",
		LeaseName:     "kubeslice-controller-lease",
		LeaseDuration: 60 * time.Second,
		RenewDeadline: 40 * time.Second,
		RetryPeriod:   15 * time.Second,
		Logger:        sugaredLogger,
	}

	le := NewLeaderElection(config)
	ctx := context.Background()

	le.OnLeadershipAcquired(ctx)
	require.True(t, le.IsLeader())

	le.OnLeadershipLost(ctx)
	require.False(t, le.IsLeader())
}

func TestLeaderElection_ThreadSafety(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := LeaderElectionConfig{
		KubeClient:    nil,
		Identity:      "test-controller-1",
		Namespace:     "kubeslice-controller",
		LeaseName:     "kubeslice-controller-lease",
		LeaseDuration: 60 * time.Second,
		RenewDeadline: 40 * time.Second,
		RetryPeriod:   15 * time.Second,
		Logger:        sugaredLogger,
	}

	le := NewLeaderElection(config)
	ctx := context.Background()

	done := make(chan bool)
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				if j%2 == 0 {
					le.OnLeadershipAcquired(ctx)
				} else {
					le.OnLeadershipLost(ctx)
				}
				_ = le.IsLeader()
				_ = le.GetLeaderIdentity()
			}
			done <- true
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	require.True(t, true)
}
