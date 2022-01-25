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
package org.ehcache.management.providers.settings;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.management.model.capabilities.descriptors.Settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class ExposedCacheSettings extends ExposedCacheBinding {

  private static final Comparator<ResourceType<?>> RESOURCE_TYPE_COMPARATOR = (o1, o2) -> o2.getTierHeight() - o1.getTierHeight();

  ExposedCacheSettings(ManagementRegistryServiceConfiguration registryConfiguration, CacheBinding cacheBinding) {
    super(registryConfiguration, cacheBinding);
  }

  @Override
  public Collection<Settings> getDescriptors() {
    final CacheConfiguration<?, ?> cacheConfig = cacheBinding.getCache().getRuntimeConfiguration();
    List<ResourceType<?>> resourceTypes = new ArrayList<>(cacheConfig.getResourcePools().getResourceTypeSet());
    resourceTypes.sort(RESOURCE_TYPE_COMPARATOR);
    Map<String, ResourceType<?>> map = new LinkedHashMap<>();
    for (ResourceType<?> type : resourceTypes) {
      map.put(type.toString(), type);
    }
    return Collections.singleton(new Settings()
        .set("cacheName", cacheBinding.getAlias())
        .set("keyType", cacheConfig.getKeyType())
        .set("valueType", cacheConfig.getValueType())
        .withEach("resourcePools", map, (settings, type) -> {
          ResourcePool pool = cacheConfig.getResourcePools().getPoolForResource(type);
          settings
              .set("level", type.getTierHeight())
              .set("persistent", pool.isPersistent());
          if (pool instanceof SizedResourcePool) {
            ResourceUnit unit = ((SizedResourcePool) pool).getUnit();
            settings
                .set("type", unit instanceof MemoryUnit ? "MEMORY" : unit instanceof EntryUnit ? "ENTRY" : unit.getClass().getSimpleName().toUpperCase())
                .set("size", ((SizedResourcePool) pool).getSize())
                .set("unit", unit.toString());
            if (Reflect.isInstance(pool, "org.ehcache.clustered.client.config.DedicatedClusteredResourcePool")) {
              settings.set("serverResource", Reflect.invoke(pool, "getFromResource", String.class));
              settings.set("clusterTier", getClusterTierExposedAlias(cacheBinding.getAlias(), pool));
            }
          } else if (Reflect.isInstance(pool, "org.ehcache.clustered.client.config.SharedClusteredResourcePool")) {
            settings.set("serverResource", Reflect.invoke(pool, "getSharedResourcePool", String.class));
            settings.set("clusterTier", getClusterTierExposedAlias(cacheBinding.getAlias(), pool));
          }
        }));
  }


  // TODO https://github.com/ehcache/ehcache3/issues/1983 -> retrieve cluster tier alias from resourcePool
  public String getClusterTierExposedAlias(String alias, ResourcePool resourcePool) {
    return alias;
  }
}
