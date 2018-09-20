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

package org.ehcache.transactions;

import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.core.util.ClassLoading;
import org.ehcache.transactions.xa.internal.XAStore;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerProvider;
import org.junit.Test;

import static java.util.Spliterators.spliterator;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;

/**
 * Ensures that a non-XA {@code CacheManager} can be created when XA classes are
 * available in classpath.
 */
public class NonXACacheTest {

  @Test
  public void testNonXA() throws Exception {

    /*
     * Ensure the XA provider classes are loadable through the ServiceLoader mechanism.
     */
    assertThat(stream(spliterator(ClassLoading.servicesOfType(ServiceFactory.class).iterator(), Long.MAX_VALUE, 0), false).map(s -> s.getServiceType()).collect(toList()),
      hasItems(XAStore.Provider.class, TransactionManagerProvider.class));

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
