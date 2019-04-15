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
package org.ehcache.config;

/**
 * A resource type.
 *
 * @param <T> associated {@code ResourcePool} type
 *
 * @see ResourcePool
 */
public interface ResourceType<T extends ResourcePool> {

  /**
   * Gets the {@link ResourcePool} type associated with this {@code ResourceType}.
   *
   * @return the {@code ResourcePool} type associated with this type
   */
  Class<T> getResourcePoolClass();

  /**
   * Indicates whether this {@code ResourceType} supports persistence.
   * <p>
   * Persistence in this context means that a {@link ResourcePool} of this {@code ResourceType} can be configured
   * so that data stored in it will survive a JVM restart.
   *
   * @return {@code true} if it supports persistence, {@code false} otherwise
   */
  boolean isPersistable();

  /**
   * Indicates whether this {@code ResourceType} requires {@link org.ehcache.spi.serialization.Serializer serialization}
   * support.
   *
   * @return {@code true} if serializers are required, {@code false} otherwise
   */
  boolean requiresSerialization();

  /**
   * Indicates the level this resource sits in the tiering system.
   * <p>
   * Higher means resource is faster and less abundant, lower means resource is slower but potentially larger.
   *
   * @return the resource tier height
   */
  int getTierHeight();

  /**
   * An enumeration of core {@link ResourceType}s in Ehcache.
   */
  enum Core implements ResourceType<SizedResourcePool> {
    /**
     * Heap: not persistable, {@link org.ehcache.spi.serialization.Serializer serialization} not required.
     */
    HEAP(false, false, 10000),
    /**
     * OffHeap: not persistable, {@link org.ehcache.spi.serialization.Serializer serialization} required.
     */
    OFFHEAP(false, true, 1000),
    /**
     * Disk: persistable, {@link org.ehcache.spi.serialization.Serializer serialization} required.
     */
    DISK(true, true, 100);


    private final boolean persistable;
    private final boolean requiresSerialization;
    private final int tierHeight;

    Core(boolean persistable, final boolean requiresSerialization, int tierHeight) {
      this.persistable = persistable;
      this.requiresSerialization = requiresSerialization;
      this.tierHeight = tierHeight;
    }

    @Override
    public Class<SizedResourcePool> getResourcePoolClass() {
      return SizedResourcePool.class;
    }

    @Override
    public boolean isPersistable() {
      return persistable;
    }

    @Override
    public boolean requiresSerialization() {
      return requiresSerialization;
    }

    @Override
    public int getTierHeight() {
      return tierHeight;
    }

    @Override
    public String toString() {
      return name().toLowerCase();
    }

  }

}
