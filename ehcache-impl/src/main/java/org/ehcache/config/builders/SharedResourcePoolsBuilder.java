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

import org.ehcache.config.Builder;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.SharedResourcePools;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.config.SharedResourcePoolsImpl;
import org.ehcache.impl.config.SizedResourcePoolImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static org.ehcache.impl.config.ResourcePoolsImpl.validateResourcePools;

/**
 * The {@code SharedResourcePoolsBuilder} enables building {@link ResourcePools} configurations using a fluent style.
 * <p>
 * As with all Ehcache builders, all instances are immutable and calling any method on the builder will return a new
 * instance without modifying the one on which the method was called.
 * This enables the sharing of builder instances without any risk of seeing them modified by code elsewhere.
 */
public class SharedResourcePoolsBuilder implements Builder<SharedResourcePools> {

  private final Map<ResourceType<?>, ResourcePool> resourcePools;

  private SharedResourcePoolsBuilder() {
    this(Collections.emptyMap());
  }

  private SharedResourcePoolsBuilder(Map<ResourceType<?>, ResourcePool> resourcePools) {
    validateResourcePools(resourcePools.values());
    this.resourcePools = unmodifiableMap(resourcePools);
  }

  /**
   * Creates a new {@code ResourcePoolsBuilder}.
   *
   * @return the new builder
   */
  public static SharedResourcePoolsBuilder newSharedResourcePoolsBuilder() {
    return new SharedResourcePoolsBuilder();
  }

  /**
   * Convenience method to get a builder from an existing {@link ResourcePools}.
   *
   * @param pools the resource pools to build from
   * @return a new builder with configuration matching the provided resource pools
   */
  public static SharedResourcePoolsBuilder newSharedResourcePoolsBuilder(ResourcePools pools) {
    SharedResourcePoolsBuilder poolsBuilder = new SharedResourcePoolsBuilder();
    for (ResourceType<?> currentResourceType : pools.getResourceTypeSet()) {
      poolsBuilder = poolsBuilder.with(pools.getPoolForResource(currentResourceType));
    }
    return poolsBuilder;
  }

  /**
   * Add the {@link ResourcePool} of {@link ResourceType} in the returned builder.
   *
   * @param resourcePool the non-{@code null} resource pool to add
   * @return a new builder with the added pool
   *
   * @throws IllegalArgumentException if the set of resource pools already contains a pool for {@code type}
   */
  public SharedResourcePoolsBuilder with(ResourcePool resourcePool) {
    final ResourceType<?> type = resourcePool.getType();
    final ResourcePool existingPool = resourcePools.get(type);
    if (existingPool != null) {
      throw new IllegalArgumentException("Can not add '" + resourcePool + "'; configuration already contains '" + existingPool + "'");
    }
    Map<ResourceType<?>, ResourcePool> newPools = new HashMap<>(resourcePools);
    newPools.put(type, resourcePool);
    return new SharedResourcePoolsBuilder(newPools);
  }

  /**
   * Add or replace the {@link ResourcePool} of {@link ResourceType} in the returned builder.
   *
   * @param resourcePool the non-{@code null} resource pool to add/replace
   * @return a new builder with the added pool
   */
  public SharedResourcePoolsBuilder withReplacing(ResourcePool resourcePool) {
    Map<ResourceType<?>, ResourcePool> newPools = new HashMap<>(resourcePools);
    newPools.put(resourcePool.getType(), resourcePool);
    return new SharedResourcePoolsBuilder(newPools);
  }

  /**
   * Add an {@link ResourceType.Core#OFFHEAP offheap} pool to be shared across multiple caches.
   *
   * @param size the pool size
   * @param unit the pool size unit
   * @return a new builder with the added pool
   *
   * @throws IllegalArgumentException if the set of resource pools already contains an offheap resource
   */
  public SharedResourcePoolsBuilder offheap(long size, MemoryUnit unit) {
    return with(new SizedResourcePoolImpl<>(ResourceType.Core.OFFHEAP, size, unit, false, true));
  }

  /**
   * Add a {@link ResourceType.Core#HEAP heap} pool to be shared across multiple caches.
   *
   * @param size the pool size
   * @param unit the pool size unit
   * @return a new builder with the added pool
   *
   * @throws IllegalArgumentException if the set of resource pools already contains a heap resource
   */
  public SharedResourcePoolsBuilder heap(long size, ResourceUnit unit) {
    // ARC Phase 2
    return with(new SizedResourcePoolImpl<>(ResourceType.Core.HEAP, size, unit, false, true));
  }

  /**
   * Builds the {@link SharedResourcePools} based on this builder's configuration.
   *
   * @return the resource pools
   */
  @Override
  public SharedResourcePoolsImpl build() {
    return new SharedResourcePoolsImpl(resourcePools);
  }
}
