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
import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.impl.config.SizedResourcePoolImpl;
import org.ehcache.config.ResourcePools;
import org.ehcache.impl.config.ResourcePoolsImpl;
import org.ehcache.config.ResourceType;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.units.MemoryUnit;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import static org.ehcache.impl.config.ResourcePoolsImpl.validateResourcePools;

/**
 * The {@code ResourcePoolsBuilder} enables building {@link ResourcePools} configurations using a fluent style.
 * <p>
 * As with all Ehcache builders, all instances are immutable and calling any method on the builder will return a new
 * instance without modifying the one on which the method was called.
 * This enables the sharing of builder instances without any risk of seeing them modified by code elsewhere.
 */
public class ResourcePoolsBuilder implements Builder<ResourcePools> {

  private final Map<ResourceType<?>, ResourcePool> resourcePools;

  private ResourcePoolsBuilder() {
    this(Collections.emptyMap());
  }

  private ResourcePoolsBuilder(Map<ResourceType<?>, ResourcePool> resourcePools) {
    validateResourcePools(resourcePools.values());
    this.resourcePools = unmodifiableMap(resourcePools);
  }

  /**
   * Creates a new {@code ResourcePoolsBuilder}.
   *
   * @return the new builder
   */
  public static ResourcePoolsBuilder newResourcePoolsBuilder() {
    return new ResourcePoolsBuilder();
  }

  /**
   * Convenience method to get a builder from an existing {@link ResourcePools}.
   *
   * @param pools the resource pools to build from
   * @return a new builder with configuration matching the provided resource pools
   */
  public static ResourcePoolsBuilder newResourcePoolsBuilder(ResourcePools pools) {
    ResourcePoolsBuilder poolsBuilder = new ResourcePoolsBuilder();
    for (ResourceType<?> currentResourceType : pools.getResourceTypeSet()) {
      poolsBuilder = poolsBuilder.with(pools.getPoolForResource(currentResourceType));
    }
    return poolsBuilder;
  }

  /**
   * Creates a new {@code ResourcePoolsBuilder} with a {@link org.ehcache.config.ResourceType.Core#HEAP heap} pool sized
   * in {@link EntryUnit#ENTRIES entries}
   *
   * @param entries the maximum number of mappings to cache
   *
   * @return a new builder with a heap configuration
   */
  public static ResourcePoolsBuilder heap(long entries) {
    return newResourcePoolsBuilder().heap(entries, EntryUnit.ENTRIES);
  }

  /**
   * Add the {@link ResourcePool} of {@link ResourceType} in the returned builder.
   *
   * @param resourcePool the non-{@code null} resource pool to add
   * @return a new builder with the added pool
   *
   * @throws IllegalArgumentException if the set of resource pools already contains a pool for {@code type}
   */
  public ResourcePoolsBuilder with(ResourcePool resourcePool) {
    final ResourceType<?> type = resourcePool.getType();
    final ResourcePool existingPool = resourcePools.get(type);
    if (existingPool != null) {
      throw new IllegalArgumentException("Can not add '" + resourcePool + "'; configuration already contains '" + existingPool + "'");
    }
    Map<ResourceType<?>, ResourcePool> newPools = new HashMap<>(resourcePools);
    newPools.put(type, resourcePool);
    return new ResourcePoolsBuilder(newPools);
  }

  /**
   * Add or replace the {@link ResourcePool} of {@link ResourceType} in the returned builder.
   *
   * @param resourcePool the non-{@code null} resource pool to add/replace
   * @return a new builder with the added pool
   */
  public ResourcePoolsBuilder withReplacing(ResourcePool resourcePool) {
    Map<ResourceType<?>, ResourcePool> newPools = new HashMap<>(resourcePools);
    newPools.put(resourcePool.getType(), resourcePool);
    return new ResourcePoolsBuilder(newPools);
  }

  /**
   * Add the {@link ResourcePool} of {@link ResourceType} in the returned builder.
   *
   * @param type the resource type
   * @param size the pool size
   * @param unit the pool size unit
   * @param persistent if the pool is to be persistent
   * @return a new builder with the added pool
   *
   * @throws IllegalArgumentException if the set of resource pools already contains a pool for {@code type}
   */
  public ResourcePoolsBuilder with(ResourceType<SizedResourcePool> type, long size, ResourceUnit unit, boolean persistent) {
    return with(new SizedResourcePoolImpl<>(type, size, unit, persistent));
  }

  /**
   * Convenience method to add a {@link org.ehcache.config.ResourceType.Core#HEAP heap} pool.
   *
   * @param size the pool size
   * @param unit the pool size unit
   * @return a new builder with the added pool
   *
   * @throws IllegalArgumentException if the set of resource pools already contains a heap resource
   */
  public ResourcePoolsBuilder heap(long size, ResourceUnit unit) {
    return with(ResourceType.Core.HEAP, size, unit, false);
  }

  /**
   * Convenience method to add an {@link org.ehcache.config.ResourceType.Core#OFFHEAP offheap} pool.
   *
   * @param size the pool size
   * @param unit the pool size unit
   * @return a new builder with the added pool
   *
   * @throws IllegalArgumentException if the set of resource pools already contains an offheap resource
   */
  public ResourcePoolsBuilder offheap(long size, MemoryUnit unit) {
    return with(ResourceType.Core.OFFHEAP, size, unit, false);
  }

  /**
   * Convenience method to add a non persistent {@link org.ehcache.config.ResourceType.Core#DISK disk} pool.
   *
   * @param size the pool size
   * @param unit the pool size unit
   * @return a new builder with the added pool
   *
   * @throws IllegalArgumentException if the set of resource pools already contains a disk resource
   */
  public ResourcePoolsBuilder disk(long size, MemoryUnit unit) {
    return disk(size, unit, false);
  }

  /**
   * Convenience method to add a {@link org.ehcache.config.ResourceType.Core#DISK disk} pool specifying persistence.
   *
   * @param size the pool size
   * @param unit the pool size unit
   * @param persistent if the pool is persistent or not
   * @return a new builder with the added pool
   *
   * @throws IllegalArgumentException if the set of resource pools already contains a disk resource
   */
  public ResourcePoolsBuilder disk(long size, MemoryUnit unit, boolean persistent) {
    return with(ResourceType.Core.DISK, size, unit, persistent);
  }

  /**
   * Builds the {@link ResourcePools} based on this builder's configuration.
   *
   * @return the resource pools
   */
  @Override
  public ResourcePools build() {
    return new ResourcePoolsImpl(resourcePools);
  }

}
