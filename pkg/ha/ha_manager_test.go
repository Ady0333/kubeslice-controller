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

func TestHAManager_NewHAManager(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HAManagerConfig{
		KubeClient:          nil,
		CRTClient:           nil,
		Identity:            "test-controller-1",
		Namespace:           "kubeslice-controller",
		LeaseName:           "kubeslice-controller-lease",
		Logger:              sugaredLogger,
		LeaseDuration:       60 * time.Second,
		RenewDeadline:       40 * time.Second,
		RetryPeriod:         15 * time.Second,
		SyncInterval:        30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		FailureThreshold:    3,
	}

	manager, err := NewHAManager(config)

	require.NoError(t, err)
	require.NotNil(t, manager)
	require.NotNil(t, manager.leaderElection)
	require.NotNil(t, manager.stateSynchronizer)
	require.NotNil(t, manager.healthChecker)
}

func TestHAManager_NewHAManager_NoLogger(t *testing.T) {
	config := HAManagerConfig{
		KubeClient:          nil,
		CRTClient:           nil,
		Identity:            "test-controller-1",
		Namespace:           "kubeslice-controller",
		LeaseName:           "kubeslice-controller-lease",
		Logger:              nil,
		LeaseDuration:       60 * time.Second,
		RenewDeadline:       40 * time.Second,
		RetryPeriod:         15 * time.Second,
		SyncInterval:        30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		FailureThreshold:    3,
	}

	manager, err := NewHAManager(config)

	require.Error(t, err)
	require.Nil(t, manager)
	require.Contains(t, err.Error(), "logger is required")
}

func TestHAManager_GetCurrentState_InitiallyStandby(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HAManagerConfig{
		KubeClient:          nil,
		CRTClient:           nil,
		Identity:            "test-controller-1",
		Namespace:           "kubeslice-controller",
		LeaseName:           "kubeslice-controller-lease",
		Logger:              sugaredLogger,
		LeaseDuration:       60 * time.Second,
		RenewDeadline:       40 * time.Second,
		RetryPeriod:         15 * time.Second,
		SyncInterval:        30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		FailureThreshold:    3,
	}

	manager, _ := NewHAManager(config)
	state := manager.GetCurrentState()

	require.Equal(t, RoleStandby, state.Role)
	require.False(t, state.IsLeader)
}

func TestHAManager_IsActive_InitiallyFalse(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HAManagerConfig{
		KubeClient:          nil,
		CRTClient:           nil,
		Identity:            "test-controller-1",
		Namespace:           "kubeslice-controller",
		LeaseName:           "kubeslice-controller-lease",
		Logger:              sugaredLogger,
		LeaseDuration:       60 * time.Second,
		RenewDeadline:       40 * time.Second,
		RetryPeriod:         15 * time.Second,
		SyncInterval:        30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		FailureThreshold:    3,
	}

	manager, _ := NewHAManager(config)
	require.False(t, manager.IsActive())
}

func TestHAManager_IsStandby_InitiallyTrue(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HAManagerConfig{
		KubeClient:          nil,
		CRTClient:           nil,
		Identity:            "test-controller-1",
		Namespace:           "kubeslice-controller",
		LeaseName:           "kubeslice-controller-lease",
		Logger:              sugaredLogger,
		LeaseDuration:       60 * time.Second,
		RenewDeadline:       40 * time.Second,
		RetryPeriod:         15 * time.Second,
		SyncInterval:        30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		FailureThreshold:    3,
	}

	manager, _ := NewHAManager(config)
	require.True(t, manager.IsStandby())
}

func TestHAManager_Stop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HAManagerConfig{
		KubeClient:          nil,
		CRTClient:           nil,
		Identity:            "test-controller-1",
		Namespace:           "kubeslice-controller",
		LeaseName:           "kubeslice-controller-lease",
		Logger:              sugaredLogger,
		LeaseDuration:       60 * time.Second,
		RenewDeadline:       40 * time.Second,
		RetryPeriod:         15 * time.Second,
		SyncInterval:        30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		FailureThreshold:    3,
	}

	manager, _ := NewHAManager(config)
	manager.Stop()

	require.True(t, manager.isStopped)
}

func TestHAManager_DefaultDurations(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HAManagerConfig{
		KubeClient:          nil,
		CRTClient:           nil,
		Identity:            "test-controller-1",
		Namespace:           "kubeslice-controller",
		LeaseName:           "kubeslice-controller-lease",
		Logger:              sugaredLogger,
		LeaseDuration:       0,
		RenewDeadline:       0,
		RetryPeriod:         0,
		SyncInterval:        0,
		HealthCheckInterval: 0,
		FailureThreshold:    0,
	}

	manager, err := NewHAManager(config)

	require.NoError(t, err)
	require.NotNil(t, manager)
	require.Equal(t, 60*time.Second, manager.currentState.LeaseDuration)
}

func TestHAManager_OnLeadershipAcquired_TransitionsToActive(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HAManagerConfig{
		KubeClient:          nil,
		CRTClient:           nil,
		Identity:            "test-controller-1",
		Namespace:           "kubeslice-controller",
		LeaseName:           "kubeslice-controller-lease",
		Logger:              sugaredLogger,
		LeaseDuration:       60 * time.Second,
		RenewDeadline:       40 * time.Second,
		RetryPeriod:         15 * time.Second,
		SyncInterval:        30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		FailureThreshold:    3,
	}

	manager, _ := NewHAManager(config)
	ctx := context.Background()

	require.True(t, manager.IsStandby())

	manager.onLeadershipAcquired(ctx)

	require.True(t, manager.IsActive())
	require.False(t, manager.IsStandby())
}

func TestHAManager_OnLeadershipLost_TransitionsToStandby(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HAManagerConfig{
		KubeClient:          nil,
		CRTClient:           nil,
		Identity:            "test-controller-1",
		Namespace:           "kubeslice-controller",
		LeaseName:           "kubeslice-controller-lease",
		Logger:              sugaredLogger,
		LeaseDuration:       60 * time.Second,
		RenewDeadline:       40 * time.Second,
		RetryPeriod:         15 * time.Second,
		SyncInterval:        30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		FailureThreshold:    3,
	}

	manager, _ := NewHAManager(config)
	ctx := context.Background()

	manager.onLeadershipAcquired(ctx)
	require.True(t, manager.IsActive())

	manager.onLeadershipLost(ctx)

	require.False(t, manager.IsActive())
	require.True(t, manager.IsStandby())
}

func TestHAManager_WaitForActivePromotion(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HAManagerConfig{
		KubeClient:          nil,
		CRTClient:           nil,
		Identity:            "test-controller-1",
		Namespace:           "kubeslice-controller",
		LeaseName:           "kubeslice-controller-lease",
		Logger:              sugaredLogger,
		LeaseDuration:       60 * time.Second,
		RenewDeadline:       40 * time.Second,
		RetryPeriod:         15 * time.Second,
		SyncInterval:        30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		FailureThreshold:    3,
	}

	manager, _ := NewHAManager(config)
	ctx := context.Background()

	go func() {
		time.Sleep(100 * time.Millisecond)
		manager.onLeadershipAcquired(ctx)
	}()

	err := manager.WaitForActivePromotion(ctx)
	require.NoError(t, err)
	require.True(t, manager.IsActive())
}
