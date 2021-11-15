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

import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.config.ResourcePool;

/**
 * Defines a resource supported by a server-based resource.
 */
public interface ClusteredResourcePool extends ResourcePool {

  @Override
  ClusteredResourceType<? extends ClusteredResourcePool> getType();

  /**
   * Converts this {@code ClusteredResourcePool} into the {@link PoolAllocation}
   * used by the cluster server.
   *
   * @return a {@code PoolAllocation} instance created from this {@code ClusteredResourcePool}
   */
  PoolAllocation getPoolAllocation();
}
