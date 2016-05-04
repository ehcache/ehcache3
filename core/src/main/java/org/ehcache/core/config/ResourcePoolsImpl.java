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

package org.ehcache.core.config;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.SizedResourcePool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of the {@link ResourcePools} interface.
 */
public class ResourcePoolsImpl implements ResourcePools {

  private final Map<ResourceType<?>, ResourcePool> pools;

  public ResourcePoolsImpl(Map<ResourceType<?>, ResourcePool> pools) {
    if (pools.isEmpty()) {
      throw new IllegalArgumentException("No resource pools defined");
    }
    validateResourcePools(pools.values());
    this.pools = pools;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <P extends ResourcePool> P getPoolForResource(ResourceType<P> resourceType) {
    return resourceType.getResourcePoolClass().cast(pools.get(resourceType));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<ResourceType<?>> getResourceTypeSet() {
    return pools.keySet();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResourcePools validateAndMerge(ResourcePools toBeUpdated) {
    // Ensure update pool types already exist in existing pools
    if(!getResourceTypeSet().containsAll(toBeUpdated.getResourceTypeSet())) {
      throw new IllegalArgumentException("Pools to be updated cannot contain previously undefined resources pools");
    }
    // Can not update OFFHEAP
    if(toBeUpdated.getResourceTypeSet().contains(ResourceType.Core.OFFHEAP)) {
      throw new UnsupportedOperationException("Updating OFFHEAP resource is not supported");
    }
    // Can not update DISK
    if(toBeUpdated.getResourceTypeSet().contains(ResourceType.Core.DISK)) {
      throw new UnsupportedOperationException("Updating DISK resource is not supported");
    }
    for(ResourceType<?> currentResourceType : toBeUpdated.getResourceTypeSet()) {
      getPoolForResource(currentResourceType).validateUpdate(toBeUpdated.getPoolForResource(currentResourceType));
    }

    Map<ResourceType<?>, ResourcePool> poolsMap = new HashMap<ResourceType<?>, ResourcePool>();
    poolsMap.putAll(pools);
    for(ResourceType<?> currentResourceType : toBeUpdated.getResourceTypeSet()) {
      ResourcePool poolForResource = toBeUpdated.getPoolForResource(currentResourceType);
      poolsMap.put(currentResourceType, poolForResource);
    }

    return new ResourcePoolsImpl(poolsMap);
  }

  /**
   * Validates some required relationships between {@link org.ehcache.config.ResourceType.Core core resources}.
   *
   * @param pools the resource pools to validate
   */
  public static void validateResourcePools(Collection<? extends ResourcePool> pools) {
    EnumMap<ResourceType.Core, SizedResourcePool> coreResources = new EnumMap<ResourceType.Core, SizedResourcePool>(ResourceType.Core.class);
    for (ResourcePool pool : pools) {
      if (pool.getType() instanceof ResourceType.Core) {
        coreResources.put((ResourceType.Core)pool.getType(), (SizedResourcePool)pool);
      }
    }

    List<SizedResourcePool> ordered = new ArrayList<SizedResourcePool>(coreResources.values());
    for (int i = 0; i < ordered.size(); i++) {
      for (int j = 0; j < i; j++) {
        SizedResourcePool upper = ordered.get(j);
        SizedResourcePool lower = ordered.get(i);

        boolean inversion;
        try {
          inversion = (upper.getUnit().compareTo(upper.getSize(), lower.getSize(), lower.getUnit()) >= 0)
                  || (lower.getUnit().compareTo(lower.getSize(), upper.getSize(), upper.getUnit()) <= 0);
        } catch (IllegalArgumentException e) {
          inversion = false;
        }
        if (inversion) {
          throw new IllegalArgumentException("Tiering Inversion: '" + upper + "' is not smaller than '" + lower + "'");
        }
      }
    }
  }
}
