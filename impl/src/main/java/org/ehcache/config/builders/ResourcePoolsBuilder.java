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

package org.ehcache.config.builders;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePoolImpl;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourcePoolsImpl;
import org.ehcache.config.ResourceType;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.units.MemoryUnit;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import static org.ehcache.config.ResourcePoolsImpl.validateResourcePools;

/**
 * @author Ludovic Orban
 */
public class ResourcePoolsBuilder implements Builder<ResourcePools> {

  private final Map<ResourceType, ResourcePool> resourcePools;

  private ResourcePoolsBuilder() {
    this(Collections.<ResourceType, ResourcePool>emptyMap());
  }

  private ResourcePoolsBuilder(Map<ResourceType, ResourcePool> resourcePools) {
    validateResourcePools(resourcePools.values());
    this.resourcePools = unmodifiableMap(resourcePools);
  }

  public static ResourcePoolsBuilder newResourcePoolsBuilder() {
    return new ResourcePoolsBuilder();
  }

  public static ResourcePoolsBuilder newResourcePoolsBuilder(ResourcePools pools) {
    ResourcePoolsBuilder poolsBuilder = new ResourcePoolsBuilder();
    for (ResourceType currentResourceType : pools.getResourceTypeSet()) {
      poolsBuilder = poolsBuilder.with(currentResourceType, pools.getPoolForResource(currentResourceType).getSize(),
          pools.getPoolForResource(currentResourceType).getUnit(), pools.getPoolForResource(currentResourceType).isPersistent());
    }
    return poolsBuilder;
  }

  public ResourcePoolsBuilder with(ResourceType type, long size, ResourceUnit unit, boolean persistent) {
    Map<ResourceType, ResourcePool> newPools = new HashMap<ResourceType, ResourcePool>(resourcePools);
    newPools.put(type, new ResourcePoolImpl(type, size, unit, persistent));
    return new ResourcePoolsBuilder(newPools);
  }

  public ResourcePoolsBuilder heap(long size, ResourceUnit unit) {
    return with(ResourceType.Core.HEAP, size, unit, false);
  }

  public ResourcePoolsBuilder offheap(long size, MemoryUnit unit) {
    return with(ResourceType.Core.OFFHEAP, size, unit, false);
  }

  public ResourcePoolsBuilder disk(long size, MemoryUnit unit) {
    return disk(size, unit, false);
  }

  public ResourcePoolsBuilder disk(long size, MemoryUnit unit, boolean persistent) {
    return with(ResourceType.Core.DISK, size, unit, persistent);
  }

  @Override
  public ResourcePools build() {
    return new ResourcePoolsImpl(resourcePools);
  }

}
