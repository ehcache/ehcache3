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

package org.ehcache.clustered.client.config;

import java.net.URI;

/**
 * The client-side portion of the {@link ClusteringServiceConfiguration} used during
 * configuration building.
 */
public interface ClusteringServiceClientSideConfiguration {
  /**
   * Gets the URI of the clustering server.
   *
   * @return the configured URI
   */
  URI getClusterUri();

  /**
   * Gets the timeout to use for cache get operations for a clustered store.
   *
   * @return the timeout to use for clustered get operations
   */
  TimeoutDuration getGetOperationTimeout();
}
