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
 * The resource pools type interface.
 *
 * @param <T> associated {@code ResourcePool} type
 */
public interface ResourceType<T extends ResourcePool> {

  /**
   * Gets the primary {@link ResourcePool} type associated with this {@code ResourceType}.
   *
   * @return the {@code ResourcePool} type associated with this type
   */
  Class<T> getResourcePoolClass();

  /**
   * Whether the resource supports persistence.
   * @return <code>true</code> if it supports persistence
   */
  boolean isPersistable();

  /**
   * Whether the resource requires serialization support.
   * @return <code>true</code> if serializers are required
   */
  boolean requiresSerialization();

  /**
   * An enumeration of resource types handled by core ehcache.
   */
  enum Core implements ResourceType<SizedResourcePool> {
    /**
     * Heap resource.
     */
    HEAP(false, false),
    /**
     * OffHeap resource.
     */
    OFFHEAP(false, true),
    /**
     * Disk resource.
     */
    DISK(true, true);


    private final boolean persistable;
    private final boolean requiresSerialization;

    Core(boolean persistable, final boolean requiresSerialization) {
      this.persistable = persistable;
      this.requiresSerialization = requiresSerialization;
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
    public String toString() {
      return name().toLowerCase();
    }
  }

}
