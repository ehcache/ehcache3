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
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class ResourcePoolsHelper {

  public static ResourcePools createHeapOnlyPools() {
    return createHeapOnlyPools(Long.MAX_VALUE);
  }

  public static ResourcePools createHeapOnlyPools(long heapSize) {
    Map<ResourceType<?>, ResourcePool> poolsMap = new HashMap<ResourceType<?>, ResourcePool>();
    poolsMap.put(ResourceType.Core.HEAP, new SizedResourcePoolImpl<SizedResourcePool>(ResourceType.Core.HEAP, heapSize, EntryUnit.ENTRIES, false));
    return new ResourcePoolsImpl(poolsMap);
  }

  public static ResourcePools createHeapOnlyPools(long heapSize, ResourceUnit resourceUnit) {
    Map<ResourceType<?>, ResourcePool> poolsMap = new HashMap<ResourceType<?>, ResourcePool>();
    poolsMap.put(ResourceType.Core.HEAP, new SizedResourcePoolImpl<SizedResourcePool>(ResourceType.Core.HEAP, heapSize, resourceUnit, false));
    return new ResourcePoolsImpl(poolsMap);
  }

  public static ResourcePools createOffheapOnlyPools(long offheapSizeInMb) {
    Map<ResourceType<?>, ResourcePool> poolsMap = new HashMap<ResourceType<?>, ResourcePool>();
    poolsMap.put(ResourceType.Core.OFFHEAP, new SizedResourcePoolImpl<SizedResourcePool>(ResourceType.Core.OFFHEAP, offheapSizeInMb, MemoryUnit.MB, false));
    return new ResourcePoolsImpl(poolsMap);
  }

  public static ResourcePools createDiskOnlyPools(long diskSize, ResourceUnit resourceUnit) {
    Map<ResourceType<?>, ResourcePool> poolsMap = new HashMap<ResourceType<?>, ResourcePool>();
    poolsMap.put(ResourceType.Core.DISK, new SizedResourcePoolImpl<SizedResourcePool>(ResourceType.Core.DISK, diskSize, resourceUnit, false));
    return new ResourcePoolsImpl(poolsMap);
  }

  public static ResourcePools createHeapDiskPools(long heapSize, long diskSizeInMb) {
    Map<ResourceType<?>, ResourcePool> poolsMap = new HashMap<ResourceType<?>, ResourcePool>();
    poolsMap.put(ResourceType.Core.HEAP, new SizedResourcePoolImpl<SizedResourcePool>(ResourceType.Core.HEAP, heapSize, EntryUnit.ENTRIES, false));
    poolsMap.put(ResourceType.Core.DISK, new SizedResourcePoolImpl<SizedResourcePool>(ResourceType.Core.DISK, diskSizeInMb, MemoryUnit.MB, false));
    return new ResourcePoolsImpl(poolsMap);
  }

  public static ResourcePools createHeapDiskPools(long heapSize, ResourceUnit heapResourceUnit, long diskSizeInMb) {
    Map<ResourceType<?>, ResourcePool> poolsMap = new HashMap<ResourceType<?>, ResourcePool>();
    poolsMap.put(ResourceType.Core.HEAP, new SizedResourcePoolImpl<SizedResourcePool>(ResourceType.Core.HEAP, heapSize, heapResourceUnit, false));
    poolsMap.put(ResourceType.Core.DISK, new SizedResourcePoolImpl<SizedResourcePool>(ResourceType.Core.DISK, diskSizeInMb, MemoryUnit.MB, false));
    return new ResourcePoolsImpl(poolsMap);
  }

}
