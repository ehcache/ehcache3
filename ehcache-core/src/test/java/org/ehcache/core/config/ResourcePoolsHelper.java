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

import java.util.Collections;
import java.util.Set;

/**
 * @author Ludovic Orban
 */
public class ResourcePoolsHelper {

  public static ResourcePools createResourcePools(long size) {
    return new ResourcePools() {
      @Override @SuppressWarnings("unchecked")
      public <P extends ResourcePool> P getPoolForResource(ResourceType<P> resourceType) {
        if (ResourceType.Core.HEAP.equals(resourceType)) {
          return (P) new SizedResourcePool() {
            @Override
            public long getSize() {
              return size;
            }

            @Override
            public ResourceUnit getUnit() {
              return EntryUnit.ENTRIES;
            }

            @Override
            public ResourceType<?> getType() {
              return ResourceType.Core.HEAP;
            }

            @Override
            public boolean isPersistent() {
              return false;
            }

            @Override
            public void validateUpdate(ResourcePool newPool) {
              //all updates are okay
            }
          };
        } else {
          return null;
        }
      }

      @Override
      public Set<ResourceType<?>> getResourceTypeSet() {
        return Collections.singleton(ResourceType.Core.HEAP);
      }

      @Override
      public ResourcePools validateAndMerge(ResourcePools toBeUpdated) throws IllegalArgumentException, UnsupportedOperationException {
        return toBeUpdated;
      }
    };
  }
}
