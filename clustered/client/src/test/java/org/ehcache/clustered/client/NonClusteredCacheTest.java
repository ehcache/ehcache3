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

package org.ehcache.clustered.client;

import org.ehcache.CacheManager;
import org.ehcache.clustered.client.internal.store.ClusteredStore;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.core.spi.service.ServiceFactory;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Ensures that a non-clustered {@code CacheManager} can be created when clustered classes are
 * available in classpath.
 */
public class NonClusteredCacheTest {

  @Test
  public void testNonClustered() throws Exception {

    /*
     * Ensure the cluster provider classes are loadable through the ServiceLoader mechanism.
     */
    Set<Class<?>> targetProviders = new HashSet<Class<?>>();
    targetProviders.add(ClusteredStore.Provider.class);
    targetProviders.add(ClusteringService.class);
    for (ServiceFactory factory : ClassLoading.libraryServiceLoaderFor(ServiceFactory.class)) {
      if (targetProviders.remove(factory.getServiceType())) {
        if (targetProviders.isEmpty()) {
          break;
        }
      }
    }
    assertThat(targetProviders, is(Matchers.empty()));

    CacheConfiguration<String, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(
        String.class,
        String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .offheap(1, MemoryUnit.MB)
            .build())
        .build();


    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);

    cacheManager.createCache("cache-1", cacheConfiguration);
    cacheManager.createCache("cache-2", cacheConfiguration);

    cacheManager.close();
  }
}
