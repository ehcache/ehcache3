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

import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 * Configuration interface for a  {@link ClusteringManagementService}.
 */
public interface ClusteringManagementServiceConfiguration<R> extends ServiceCreationConfiguration<ClusteringManagementService, R> {

  /**
   * @return The alias of the executor used to run management call queries.
   */
  String getManagementCallExecutorAlias();

  /**
   * @return The maximum size of the bounded queue used to stock management call requests before executing them
   */
  int getManagementCallQueueSize();

  /**
   * @return The timeout in seconds for management call operations
   */
  long getManagementCallTimeoutSec();
}
