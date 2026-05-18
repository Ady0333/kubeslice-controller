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

func TestStateSynchronizer_NewStateSynchronizer(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := StateSynchronizerConfig{
		KubeClient:   nil,
		Logger:       sugaredLogger,
		SyncInterval: 30 * time.Second,
	}

	ss := NewStateSynchronizer(config)

	require.NotNil(t, ss)
	require.Equal(t, 30*time.Second, ss.syncInterval)
	require.False(t, ss.isSynced)
}

func TestStateSynchronizer_IsSynced_InitiallyFalse(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := StateSynchronizerConfig{
		KubeClient:   nil,
		Logger:       sugaredLogger,
		SyncInterval: 30 * time.Second,
	}

	ss := NewStateSynchronizer(config)
	require.False(t, ss.IsSynced())
}

func TestStateSynchronizer_SyncResource(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := StateSynchronizerConfig{
		KubeClient:   nil,
		Logger:       sugaredLogger,
		SyncInterval: 30 * time.Second,
	}

	ss := NewStateSynchronizer(config)
	ctx := context.Background()

	err := ss.SyncResource(ctx, "SliceConfig", "test-slice", "default")
	require.NoError(t, err)

	lastSyncTime := ss.GetLastSyncTime("SliceConfig/default/test-slice")
	require.NotEqual(t, time.Time{}, lastSyncTime)
}

func TestStateSynchronizer_GetLastSyncTime_NonExistent(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := StateSynchronizerConfig{
		KubeClient:   nil,
		Logger:       sugaredLogger,
		SyncInterval: 30 * time.Second,
	}

	ss := NewStateSynchronizer(config)

	lastSyncTime := ss.GetLastSyncTime("NonExistent")
	require.Equal(t, time.Time{}, lastSyncTime)
}

func TestStateSynchronizer_StartStop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := StateSynchronizerConfig{
		KubeClient:   nil,
		Logger:       sugaredLogger,
		SyncInterval: 1 * time.Second,
	}

	ss := NewStateSynchronizer(config)
	ctx := context.Background()

	err := ss.StartSync(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	ss.StopSync()
	require.NotNil(t, ss.ticker)
}

func TestStateSynchronizer_WaitForSync_Immediate(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := StateSynchronizerConfig{
		KubeClient:   nil,
		Logger:       sugaredLogger,
		SyncInterval: 30 * time.Second,
	}

	ss := NewStateSynchronizer(config)
	ss.isSynced = true

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := ss.WaitForSync(ctx, 5*time.Second)
	require.NoError(t, err)
}

func TestStateSynchronizer_WaitForSync_Timeout(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	config := StateSynchronizerConfig{
		KubeClient:   nil,
		Logger:       sugaredLogger,
		SyncInterval: 30 * time.Second,
	}

	ss := NewStateSynchronizer(config)
	ss.isSynced = false

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ss.WaitForSync(ctx, 100*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout")
}
