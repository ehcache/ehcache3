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

package org.ehcache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.internal.store.heap.service.OnHeapStoreServiceConfig;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.ehcache.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.CacheManagerBuilder.persistence;
import static org.ehcache.StandaloneCacheBuilder.newCacheBuilder;
import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

/**
 * @author Alex Snaps
 */
@SuppressWarnings("unused")
public class GettingStarted {

  /**
   * If you change this somehow, you probably want to fix the /README.adoc as well!
   * The callouts are also used in docs/user/index.adoc
   */

  @Test
  public void testWikiExample() {

    final CacheManager cacheManager
        = newCacheManagerBuilder() // <1>
        .withCache("preConfigured",
            newCacheConfigurationBuilder().buildConfig(Long.class, String.class)) // <2>
        .build(); // <3>

    final Cache<Long, String> preConfigured =
        cacheManager.getCache("preConfigured", Long.class, String.class); // <4>

    final Cache<Long, String> myCache = cacheManager.createCache("myCache", // <5>
        newCacheConfigurationBuilder().buildConfig(Long.class, String.class));

    myCache.put(1L, "da one!"); // <6>
    final String value = myCache.get(1L); // <7>

    cacheManager.removeCache("preConfigured"); // <8>

    final StandaloneCache<Long, String> standaloneCache =
        newCacheBuilder(Long.class, String.class,
            LoggerFactory.getLogger(Ehcache.class + "-" + "GettingStarted"))
        .build(); // <9>
    standaloneCache.init(); // <10>

    cacheManager.close(); // <11>

    standaloneCache.close(); // <12>

    PersistentCacheManager persistentCacheManager = newCacheManagerBuilder()
        .with(persistence(System.getProperty("java.io.tmpdir") + "/myData")) // <13>
        .build();

    persistentCacheManager.close();
  }

  @Test
  public void testTieredStore() throws Exception {
    CacheConfiguration<Long, String> tieredCacheConfiguration = newCacheConfigurationBuilder()
        .persistenceMode(CacheConfiguration.PersistenceMode.SWAP)
        .withResourcePools(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).disk(100, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = newCacheManagerBuilder().withCache("tiered-cache", tieredCacheConfiguration).build();

    Cache<Long, String> tieredCache = cacheManager.getCache("tiered-cache", Long.class, String.class);

    tieredCache.put(1L, "one");

    assertThat(tieredCache.get(1L), equalTo("one")); // probably coming from disk
    assertThat(tieredCache.get(1L), equalTo("one")); // probably coming from heap

    cacheManager.close();
  }

  @Test
  public void testPersistentDiskCache() {
    CacheConfiguration<Long, String> cacheConfiguration = newCacheConfigurationBuilder()
        .persistenceMode(CacheConfiguration.PersistenceMode.CREATE_IF_ABSENT)
        .withResourcePools(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).disk(100, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    PersistentCacheManager persistentCacheManager = newCacheManagerBuilder()
        .with(new PersistenceConfiguration(new File("build/persistent-cache-data")))
        .withCache("persistent-cache", cacheConfiguration)
        .build();

    Cache<Long, String> cache = persistentCacheManager.getCache("persistent-cache", Long.class, String.class);

    // Comment the following line on subsequent run and see the test pass
    cache.put(42L, "That's the answer!");
    assertThat(cache.get(42L), is("That's the answer!"));

    // Uncomment the following line to nuke the disk store
//    persistentCacheManager.destroyCache("persistent-cache");

    persistentCacheManager.close();
  }

  @Test
  public void testStoreByValue() {
    CacheManager cacheManager = newCacheManagerBuilder().build();

    final Cache<Long, String> cache1 = cacheManager.createCache("cache1",
        newCacheConfigurationBuilder().withResourcePools(newResourcePoolsBuilder().heap(1, EntryUnit.ENTRIES).build()).buildConfig(Long.class, String.class));
    performAssertions(cache1, true);

    final Cache<Long, String> cache2 = cacheManager.createCache("cache2",
        newCacheConfigurationBuilder().addServiceConfig(new OnHeapStoreServiceConfig().storeByValue(true)).buildConfig(Long.class, String.class));
    performAssertions(cache2, false);

    final Cache<Long, String> cache3 = cacheManager.createCache("cache3",
        newCacheConfigurationBuilder().addServiceConfig(new OnHeapStoreServiceConfig().storeByValue(false)).buildConfig(Long.class, String.class));
    performAssertions(cache3, true);

    cacheManager.close();
  }

  private void performAssertions(Cache<Long, String> cache, boolean same) {
    cache.put(1L, "one");
    String s1 = cache.get(1L);
    String s2 = cache.get(1L);
    String s3 = cache.get(1L);

    assertThat(s1 == s2, is(same));
    assertThat(s2 == s3, is(same));
  }

}
