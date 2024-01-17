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
import org.ehcache.config.ResourceType;
import org.ehcache.config.SharedResourcePools;
import org.ehcache.core.HumanReadable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of the {@link SharedResourcePools} interface.
 */
public class SharedResourcePoolsImpl implements SharedResourcePools, HumanReadable {

  private final Map<ResourceType<?>, ResourcePool> pools;

  public SharedResourcePoolsImpl(Map<ResourceType<?>, ResourcePool> pools) {
    requireNonNull(pools);
    validateSharedResourcePools(pools.values());
    this.pools = pools;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<ResourceType<?>> getResourceTypeSet() {
    return pools.keySet();
  }

  @Override
  public Map<ResourceType<?>, ResourcePool> getSharedResourcePools () {
    return pools;
  }

  @Override
  public Collection<ResourcePool> getResourcePools () {
    return pools.values();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SharedResourcePools validateAndMerge(SharedResourcePools toBeUpdated) {
    //TODO: what validations are required on shared resource pools to permit merging?
    return null;
  }

  /**
   * Validates some required relationships between {@link ResourceType.Core core resources}.
   *
   * @param pools the resource pools to validate
   */
  public static void validateSharedResourcePools(Collection<? extends ResourcePool> pools) {
    //TODO: what validations may be relevant for shared resource pools?
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
    return "pools: " + "\n    " + poolsToStringBuilder;
  }
}
