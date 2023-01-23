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

package org.ehcache.clustered.client.config.builders;

import org.ehcache.clustered.client.config.SharedClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.SharedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.ClusteredResourcePoolImpl;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.clustered.client.config.DedicatedClusteredResourcePool;

/**
 * Constructs a {@link org.ehcache.config.ResourcePool ResourcePool} for a clustered resource.
 */
public final class ClusteredResourcePoolBuilder {

  /** Private, niladic constructor to prevent instantiation. */
  private ClusteredResourcePoolBuilder() {
  }

  /**
   * Creates a new clustered resource pool using dedicated clustered resources.
   *
   * @param size       the size
   * @param unit       the unit for the size
   */
  public static DedicatedClusteredResourcePool clusteredDedicated(long size, MemoryUnit unit) {
    return new DedicatedClusteredResourcePoolImpl(null, size, unit);
  }

  /**
   * Creates a new clustered resource pool using dedicated clustered resources.
   *
   * @param fromResource the name of the server-based resource from which this dedicated resource pool
                     is reserved; may be {@code null}
   * @param size       the size
   * @param unit       the unit for the size
   */
  public static DedicatedClusteredResourcePool clusteredDedicated(String fromResource, long size, MemoryUnit unit) {
    return new DedicatedClusteredResourcePoolImpl(fromResource, size, unit);
  }

  /**
   * Creates a new resource pool based on the provided parameters.
   *
   * @param sharedResource the non-{@code null} name of the server-based resource pool whose space is shared
   *                       by this pool
   */
  public static SharedClusteredResourcePool clusteredShared(String sharedResource) {
    return new SharedClusteredResourcePoolImpl(sharedResource);
  }

  /**
   * Creates a resource pool that inherits the resources already configured on the server.
   */
  public static ClusteredResourcePool clustered() {
    return new ClusteredResourcePoolImpl();
  }
}
