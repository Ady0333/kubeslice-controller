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
	"time"
)

type ControllerRole string

const (
	RoleActive  ControllerRole = "Active"
	RoleStandby ControllerRole = "Standby"
)

type ControllerState struct {
	Role              ControllerRole
	IsLeader          bool
	LeaderIdentity    string
	LastHeartbeat     time.Time
	LeaseDuration     time.Duration
	RenewDeadline     time.Time
	TransitionTime    time.Time
}

type ILeaderElection interface {
	Start(ctx context.Context) error
	Stop()
	IsLeader() bool
	GetLeaderIdentity() string
	OnLeadershipAcquired(ctx context.Context)
	OnLeadershipLost(ctx context.Context)
}

type IStateSynchronizer interface {
	StartSync(ctx context.Context) error
	StopSync()
	SyncResource(ctx context.Context, resourceType string, resourceName string, namespace string) error
	GetLastSyncTime(resourceType string) time.Time
	IsSynced() bool
}

type IHealthChecker interface {
	StartHealthCheck(ctx context.Context) error
	StopHealthCheck()
	IsHealthy() bool
	GetHealthStatus() HealthStatus
}

type HealthStatus struct {
	IsHealthy           bool
	LastCheck           time.Time
	ConsecutiveFailures int
	LastError           error
}

type IHAManager interface {
	Start(ctx context.Context) error
	Stop()
	GetCurrentState() ControllerState
	IsActive() bool
	IsStandby() bool
	WaitForActivePromotion(ctx context.Context) error
}
