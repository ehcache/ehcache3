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
 * @author Ludovic Orban
 */
public interface ResourceType {

  /**
   * Whether the resource supports persistence
   * @return <code>true</code> if it supports persistence
   */
  boolean isPersistable();

  /**
   * An enumeration of resource types handled by core ehcache.
   */
  enum Core implements ResourceType {
    /**
     * Heap resource.
     */
    HEAP(false),
    /**
     * OffHeap resource.
     */
    OFFHEAP(false),
    /**
     * Disk resource.
     */
    DISK(true);


    private final boolean persistable;

    Core(boolean persistable) {
      this.persistable = persistable;
    }

    @Override
    public boolean isPersistable() {
      return persistable;
    }

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

}
