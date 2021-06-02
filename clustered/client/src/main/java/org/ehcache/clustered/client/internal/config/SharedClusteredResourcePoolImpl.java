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

package org.ehcache.clustered.client.internal.config;

import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.SharedClusteredResourcePool;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.config.ResourcePool;
import org.ehcache.impl.config.AbstractResourcePool;

/**
 * Implementation for {@link SharedClusteredResourcePool}.
 */
public class SharedClusteredResourcePoolImpl
    extends AbstractResourcePool<SharedClusteredResourcePool, ClusteredResourceType<SharedClusteredResourcePool>>
    implements SharedClusteredResourcePool {

  private final String sharedResourcePool;

  /**
   * Creates a new resource pool based on the provided parameters.
   *
   * @param sharedResourcePool the non-{@code null} name of the server-based resource pool whose space is shared
   *                       by this pool
   */
  public SharedClusteredResourcePoolImpl(final String sharedResourcePool) {
    super(ClusteredResourceType.Types.SHARED, true);

    if (sharedResourcePool == null) {
      throw new NullPointerException("sharedResourcePool identifier can not be null");
    }
    this.sharedResourcePool = sharedResourcePool;
  }

  @Override
  public String getSharedResourcePool() {
    return this.sharedResourcePool;
  }

  @Override
  public PoolAllocation getPoolAllocation() {
    return new PoolAllocation.Shared(this.getSharedResourcePool());
  }

  @Override
  public void validateUpdate(final ResourcePool newPool) {
    throw new UnsupportedOperationException("Updating CLUSTERED resource is not supported");
  }

  @Override
  public String toString() {
    return "Pool {"
        + "sharedResourcePool='" + sharedResourcePool + '\''
        + " " + getType() + "}";
  }
}
