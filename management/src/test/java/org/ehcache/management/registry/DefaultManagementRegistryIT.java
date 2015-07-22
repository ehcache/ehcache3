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
package org.ehcache.management.registry;

import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.management.ManagementRegistry;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;

/**
 * @author Ludovic Orban
 */
public class DefaultManagementRegistryIT {

  @Test
  public void testSingleAgentMultipleCacheManagers() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry();


    CapabilitiesCollectorService capabilitiesCollectorService1 = new CapabilitiesCollectorService();
    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(capabilitiesCollectorService1)
        .using(managementRegistry)
        .build(true);

    CapabilitiesCollectorService capabilitiesCollectorService2 = new CapabilitiesCollectorService();
    CacheManager cacheManager2 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(capabilitiesCollectorService2)
        .using(managementRegistry)
        .build(true);


    assertThat(capabilitiesCollectorService1.managementRegistry, sameInstance(capabilitiesCollectorService2.managementRegistry));
    assertThat(capabilitiesCollectorService1.managementRegistry, sameInstance(managementRegistry));

    cacheManager2.close();
    cacheManager1.close();
  }

  static class CapabilitiesCollectorService implements Service {

    public volatile ManagementRegistry managementRegistry;

    @Override
    public void start(ServiceProvider serviceProvider) {
      managementRegistry = serviceProvider.findService(ManagementRegistry.class);
    }

    @Override
    public void stop() {
      managementRegistry = null;
    }

  }


}

