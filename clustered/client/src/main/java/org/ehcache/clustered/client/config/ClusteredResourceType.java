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

package org.ehcache.clustered.client.config;

import org.ehcache.config.ResourceType;

/**
 * Defines the clustered {@link ResourceType}.
 */
public interface ClusteredResourceType<P extends ClusteredResourcePool> extends ResourceType<P> {

  final class Types {

    /**
     * Identifies the {@code clustered-dedicated} {@link ResourceType}.
     */
    public static final ClusteredResourceType<DedicatedClusteredResourcePool> DEDICATED =
      new BaseClusteredResourceType<>("DEDICATED", DedicatedClusteredResourcePool.class);

    /**
     * Identifies the {@code cluster-shared} {@link ResourceType}.
     */
    public static final ClusteredResourceType<SharedClusteredResourcePool> SHARED =
      new BaseClusteredResourceType<>("SHARED", SharedClusteredResourcePool.class);

    /**
     * Identifies the {@code clustered} {@link ResourceType}.
     */
    public static final ClusteredResourceType<ClusteredResourcePool> UNKNOWN =
      new BaseClusteredResourceType<>("UNKNOWN", ClusteredResourcePool.class);

    /**
     * Returns an array containing the constants of the {@link ClusteredResourceType}.
     *
     * @return an array containing the constants of {@code ClusteredResourceType} in the order declared
     */
    public static ClusteredResourceType<? extends ClusteredResourcePool>[] values() {
      return new ClusteredResourceType<?>[] {DEDICATED, SHARED, UNKNOWN};
    }

    /**
     * The base on which {@link ClusteredResourceType} identifiers are built.
     *
     * @param <P> the {@link ClusteredResourcePool} type associated with this resource type
     */
    private static final class BaseClusteredResourceType<P extends ClusteredResourcePool> implements ClusteredResourceType<P> {
      private final String name;
      private final Class<P> resourcePoolClass;

      private BaseClusteredResourceType(final String name, final Class<P> resourcePoolClass) {
        this.name = name;
        this.resourcePoolClass = resourcePoolClass;
      }

      @Override
      public Class<P> getResourcePoolClass() {
        return resourcePoolClass;
      }

      @Override
      public boolean isPersistable() {
        return true;
      }

      @Override
      public boolean requiresSerialization() {
        return true;
      }

      @Override
      public int getTierHeight() {
        return 10;
      }

      @Override
      public String toString() {
        return "clustered-" + this.name.toLowerCase();
      }
    }
  }
}
