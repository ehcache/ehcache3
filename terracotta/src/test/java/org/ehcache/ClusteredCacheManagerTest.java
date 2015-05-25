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

import org.ehcache.config.BaseCacheConfiguration;
import org.ehcache.config.BaseClusteredCacheSharedConfiguration;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.config.ClusteredCacheSharedConfiguration;
import org.ehcache.config.ClusteredCacheSharedRuntimeConfiguration;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.TerracottaBaseCacheConfiguration;
import org.ehcache.config.TerracottaCacheRuntimeConfiguration;
import org.ehcache.config.TerracottaConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Expiry;
import org.junit.Test;

import java.net.URI;

/**
 * @author Alex Snaps
 */
public class ClusteredCacheManagerTest {

  @Test
  public void testNothingButAPI() {

    final URI terracottaURI = sourceThat();
    final TerracottaConfiguration tcConfig = new TerracottaConfiguration(terracottaURI);
    tcConfig.setDefaultCacheSize(42, MemoryUnit.GB, true); // not saying we should have this default here or at all


    // Pass TC Config in, and retrieve a PersistentCacheManager to get access to live cycle stuff
    final PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(tcConfig)
        .build(true);

    // UnclusteredCache
    final Cache<String, Object> foo = cacheManager.createCache("foo",
        new BaseCacheConfiguration<String, Object>(
            String.class,
            Object.class,
            (EvictionVeto)null,        // Whatever this is
            (EvictionPrioritizer)null, // Whatever this is
            (Expiry)null,              // Whatever this is
            this.getClass().getClassLoader(),
            ResourcePoolsBuilder
                .newResourcePoolsBuilder()
                .heap(500, EntryUnit.ENTRIES)
                .build())
    );

    // Clustered with sizing defaults (say moving from the unclustered cache above)
    final Cache<String, Object> bar = cacheManager.createCache("bar",
        new TerracottaBaseCacheConfiguration<String, Object>( // only this line changes
            String.class,
            Object.class,
            (EvictionVeto)null,        // Whatever this is (or was) as long as it's "portable"
            (EvictionPrioritizer)null, // Whatever this is (or was) as long as it's "portable"
            (Expiry)null,              // Whatever this is (or was) as long as it's "portable"
            this.getClass().getClassLoader(),
            ResourcePoolsBuilder              // Local tier(s) only ?!
                .newResourcePoolsBuilder()
                .heap(500, EntryUnit.ENTRIES)
                .build())
    );

    // Clustered with all specified config stuff
    final Cache<String, Object> baz = cacheManager.createCache("baz",
        new TerracottaBaseCacheConfiguration<String, Object>(
            new BaseClusteredCacheSharedConfiguration<String, Object>(
                String.class,
                Object.class,
                (EvictionVeto)null,
                (EvictionPrioritizer)null,
                (Expiry)null,
                2, MemoryUnit.TB, false, // 2 TB non-persistent, resourcePool aren't (yet?) "splittable"
                false, // not transactional
                BaseClusteredCacheSharedConfiguration.CacheLoaderWriter.NONE // no CacheLoaderWriter
            ),
            this.getClass().getClassLoader(),
            ResourcePoolsBuilder
                .newResourcePoolsBuilder()
                .heap(500, EntryUnit.ENTRIES)
                .build())
    );

    // Accessing an existing clustered cache (or with all defaults? but that could lead to unclear races)
    final Cache<String, Object> simple = cacheManager.createCache("simple",
        new TerracottaBaseCacheConfiguration<String, Object>(
            new BaseClusteredCacheSharedConfiguration<String, Object>(
                String.class,
                Object.class),
            this.getClass().getClassLoader(),
            ResourcePoolsBuilder              // Local tier(s) only ?!
                .newResourcePoolsBuilder()
                .heap(500, EntryUnit.ENTRIES)
                .build()));

    // Share runtime config change
    final CacheRuntimeConfiguration<String, Object> runtimeConfiguration = simple.getRuntimeConfiguration();
    if(runtimeConfiguration instanceof TerracottaCacheRuntimeConfiguration) { // instanceof check to see whether it's clustered?!
      TerracottaCacheRuntimeConfiguration tcCacheConfig = (TerracottaCacheRuntimeConfiguration)runtimeConfiguration;
      ClusteredCacheSharedRuntimeConfiguration serverSideConfig = tcCacheConfig.getClusteredCacheSharedRuntimeConfiguration();
      serverSideConfig.setSomething();
    }

    // life cycle stuff enhanced through the PersistentCacheManager interface
    cacheManager.destroyCache("bar");
    cacheManager.destroyCache("baz");
    cacheManager.destroyCache("simple");
  }

  public ClusteredCacheSharedConfiguration<String, Object> clusteredConfig() {
    final ClusteredCacheSharedConfiguration<String, Object> cacheSharedConfiguration = sourceThat();
    return cacheSharedConfiguration;
  }

  private <T> T sourceThat() {
    return null;
  }
}
