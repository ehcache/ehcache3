/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehcache.management.cluster;

public class DefaultClusteringManagementServiceConfiguration implements ClusteringManagementServiceConfiguration<Void> {

  private String managementCallExecutorAlias = "managementCallExecutor";
  private int managementCallQueueSize = 1024;
  private long managementCallTimeoutSec = 5;

  @Override
  public String getManagementCallExecutorAlias() {
    return managementCallExecutorAlias;
  }

  public DefaultClusteringManagementServiceConfiguration setManagementCallExecutorAlias(String managementCallExecutorAlias) {
    this.managementCallExecutorAlias = managementCallExecutorAlias;
    return this;
  }

  @Override
  public int getManagementCallQueueSize() {
    return managementCallQueueSize;
  }

  public DefaultClusteringManagementServiceConfiguration setManagementCallQueueSize(int managementCallQueueSize) {
    this.managementCallQueueSize = managementCallQueueSize;
    return this;
  }

  @Override
  public long getManagementCallTimeoutSec() {
    return managementCallTimeoutSec;
  }

  public DefaultClusteringManagementServiceConfiguration setManagementCallTimeoutSec(long managementCallTimeoutSec) {
    this.managementCallTimeoutSec = managementCallTimeoutSec;
    return this;
  }

  @Override
  public Class<ClusteringManagementService> getServiceType() {
    return ClusteringManagementService.class;
  }
}
