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

package org.ehcache.impl.config;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.core.HumanReadable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Implementation of the {@link ResourcePools} interface.
 */
public class ResourcePoolsImpl implements ResourcePools, HumanReadable {

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
    Set<ResourceType<?>> resourceTypeSet = toBeUpdated.getResourceTypeSet();

    // Ensure update pool types already exist in existing pools
    if(!getResourceTypeSet().containsAll(resourceTypeSet)) {
      throw new IllegalArgumentException("Pools to be updated cannot contain previously undefined resources pools");
    }
    // Can not update OFFHEAP
    if(resourceTypeSet.contains(ResourceType.Core.OFFHEAP)) {
      throw new UnsupportedOperationException("Updating OFFHEAP resource is not supported");
    }
    // Can not update DISK
    if(resourceTypeSet.contains(ResourceType.Core.DISK)) {
      throw new UnsupportedOperationException("Updating DISK resource is not supported");
    }
    for(ResourceType<?> currentResourceType : resourceTypeSet) {
      getPoolForResource(currentResourceType).validateUpdate(toBeUpdated.getPoolForResource(currentResourceType));
    }

    Map<ResourceType<?>, ResourcePool> poolsMap = new HashMap<>(pools.size() + resourceTypeSet.size());
    poolsMap.putAll(pools);
    for(ResourceType<?> currentResourceType : resourceTypeSet) {
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
    List<SizedResourcePool> ordered = new ArrayList<>(pools.size());
    for(ResourcePool pool : pools) {
      if (pool instanceof SizedResourcePool) {
        ordered.add((SizedResourcePool)pool);
      }
    }
    ordered.sort((o1, o2) -> {
      int retVal = o2.getType().getTierHeight() - o1.getType().getTierHeight();
      if (retVal == 0) {
        return o1.toString().compareTo(o2.toString());
      } else {
        return retVal;
      }
    });

    for (int i = 0; i < ordered.size(); i++) {
      for (int j = 0; j < i; j++) {
        SizedResourcePool upper = ordered.get(j);
        SizedResourcePool lower = ordered.get(i);

        boolean inversion;
        boolean ambiguity;
        try {
          ambiguity = upper.getType().getTierHeight() == lower.getType().getTierHeight();
          inversion = (upper.getUnit().compareTo(upper.getSize(), lower.getSize(), lower.getUnit()) >= 0)
                      || (lower.getUnit().compareTo(lower.getSize(), upper.getSize(), upper.getUnit()) <= 0);
        } catch (IllegalArgumentException e) {
          ambiguity = false;
          inversion = false;
        }
        if (ambiguity) {
          throw new IllegalArgumentException("Tiering Ambiguity: '" + upper + "' has the same tier height as '" + lower + "'");
        }
        if (inversion) {
          throw new IllegalArgumentException("Tiering Inversion: '" + upper + "' is not smaller than '" + lower + "'");
        }
      }
    }
  }

  @Override
  public String readableString() {

    Map<ResourceType<?>, ResourcePool> sortedPools = new TreeMap<>(
      (o1, o2) -> o2.getTierHeight() - o1.getTierHeight()
    );
    sortedPools.putAll(pools);

    StringBuilder poolsToStringBuilder = new StringBuilder();

    for (Map.Entry<ResourceType<?>, ResourcePool> poolEntry : sortedPools.entrySet()) {
      poolsToStringBuilder
          .append(poolEntry.getKey())
          .append(": ")
          .append("\n        ")
          .append("size: ")
          .append(poolEntry.getValue() instanceof HumanReadable ? ((HumanReadable) poolEntry.getValue()).readableString() : poolEntry.getValue())
          .append("\n        ")
          .append("tierHeight: ")
          .append(poolEntry.getKey().getTierHeight())
          .append("\n    ");
    }

    if (poolsToStringBuilder.length() > 4) {
      poolsToStringBuilder.delete(poolsToStringBuilder.length() - 5, poolsToStringBuilder.length());
    }

    return "pools: " + "\n    " +
        poolsToStringBuilder.toString();
  }
}
