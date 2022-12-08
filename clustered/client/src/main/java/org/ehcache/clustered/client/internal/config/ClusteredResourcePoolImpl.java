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
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.config.ResourcePool;
import org.ehcache.impl.config.AbstractResourcePool;

/**
 * Implementation for {@link ClusteredResourcePool}.
 */
public class ClusteredResourcePoolImpl
        extends AbstractResourcePool<ClusteredResourcePool, ClusteredResourceType<ClusteredResourcePool>>
        implements ClusteredResourcePool {

  public ClusteredResourcePoolImpl() {
    super(ClusteredResourceType.Types.UNKNOWN, true);
  }

  @Override
  public PoolAllocation getPoolAllocation() {
    return new PoolAllocation.Unknown();
  }

  @Override
  public void validateUpdate(ResourcePool newPool) {
    throw new UnsupportedOperationException("Updating CLUSTERED resource is not supported");
  }

  @Override
  public String toString() {
    return "Pool {"
          + "ResourcePool type: "
          + getType()
          + (isPersistent() ? "(persistent)}" : "}");
  }

}
