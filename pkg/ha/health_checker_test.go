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

func TestHealthChecker_NewHealthChecker(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HealthCheckerConfig{
		KubeClient:       nil,
		Logger:           sugaredLogger,
		Namespace:        "kubeslice-controller",
		LeaseName:        "kubeslice-controller-lease",
		CheckInterval:    10 * time.Second,
		FailureThreshold: 3,
	}

	hc := NewHealthChecker(config)

	require.NotNil(t, hc)
	require.Equal(t, "kubeslice-controller", hc.namespace)
	require.Equal(t, "kubeslice-controller-lease", hc.leaseName)
	require.Equal(t, 10*time.Second, hc.checkInterval)
	require.Equal(t, 3, hc.failureThreshold)
	require.True(t, hc.status.IsHealthy)
}

func TestHealthChecker_IsHealthy_InitiallyTrue(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HealthCheckerConfig{
		KubeClient:       nil,
		Logger:           sugaredLogger,
		Namespace:        "kubeslice-controller",
		LeaseName:        "kubeslice-controller-lease",
		CheckInterval:    10 * time.Second,
		FailureThreshold: 3,
	}

	hc := NewHealthChecker(config)
	require.True(t, hc.IsHealthy())
}

func TestHealthChecker_GetHealthStatus(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HealthCheckerConfig{
		KubeClient:       nil,
		Logger:           sugaredLogger,
		Namespace:        "kubeslice-controller",
		LeaseName:        "kubeslice-controller-lease",
		CheckInterval:    10 * time.Second,
		FailureThreshold: 3,
	}

	hc := NewHealthChecker(config)
	status := hc.GetHealthStatus()

	require.True(t, status.IsHealthy)
	require.Equal(t, 0, status.ConsecutiveFailures)
	require.Nil(t, status.LastError)
}

func TestHealthChecker_StartStop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HealthCheckerConfig{
		KubeClient:       nil,
		Logger:           sugaredLogger,
		Namespace:        "kubeslice-controller",
		LeaseName:        "kubeslice-controller-lease",
		CheckInterval:    100 * time.Millisecond,
		FailureThreshold: 3,
	}

	hc := NewHealthChecker(config)
	ctx := context.Background()

	err := hc.StartHealthCheck(ctx)
	require.NoError(t, err)

	time.Sleep(150 * time.Millisecond)

	hc.StopHealthCheck()
	require.NotNil(t, hc.ticker)
}

func TestHealthChecker_WaitForHealthy_AlreadyHealthy(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HealthCheckerConfig{
		KubeClient:       nil,
		Logger:           sugaredLogger,
		Namespace:        "kubeslice-controller",
		LeaseName:        "kubeslice-controller-lease",
		CheckInterval:    10 * time.Second,
		FailureThreshold: 3,
	}

	hc := NewHealthChecker(config)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := hc.WaitForHealthy(ctx, 5*time.Second)
	require.NoError(t, err)
}

func TestHealthChecker_WaitForHealthy_Timeout(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HealthCheckerConfig{
		KubeClient:       nil,
		Logger:           sugaredLogger,
		Namespace:        "kubeslice-controller",
		LeaseName:        "kubeslice-controller-lease",
		CheckInterval:    10 * time.Second,
		FailureThreshold: 3,
	}

	hc := NewHealthChecker(config)
	hc.status.IsHealthy = false

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := hc.WaitForHealthy(ctx, 100*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout")
}

func TestHealthChecker_DefaultCheckInterval(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := HealthCheckerConfig{
		KubeClient:       nil,
		Logger:           sugaredLogger,
		Namespace:        "kubeslice-controller",
		LeaseName:        "kubeslice-controller-lease",
		CheckInterval:    0,
		FailureThreshold: 0,
	}

	hc := NewHealthChecker(config)
	require.Equal(t, 10*time.Second, hc.checkInterval)
	require.Equal(t, 3, hc.failureThreshold)
}
