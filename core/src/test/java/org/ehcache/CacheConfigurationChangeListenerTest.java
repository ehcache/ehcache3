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
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import org.ehcache.config.units.MemoryUnit;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

/**
 * @author rism
 */
public class CacheConfigurationChangeListenerTest {
  private Store<Object, Object> store;
  private CacheEventNotificationService<Object, Object> eventNotifier;
  private RuntimeConfiguration<Object, Object> runtimeConfiguration;
  private CacheConfiguration<Object, Object> config;
  private Ehcache<Object, Object> cache;

  @SuppressWarnings({ "unchecked"})
  @Before
  public void setUp() throws Exception {
    this.store = mock(Store.class);
    this.eventNotifier = mock(CacheEventNotificationService.class);
    CacheLoaderWriter<Object, Object> loaderWriter = mock(CacheLoaderWriter.class);
    this.config = newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().disk(10L, MemoryUnit.MB).heap(2L, EntryUnit.ENTRIES))
        .buildConfig(Object.class, Object.class);
    this.runtimeConfiguration = new RuntimeConfiguration<Object, Object>(this.config, this.eventNotifier);
    this.cache = new Ehcache<Object, Object>(runtimeConfiguration, store, loaderWriter, eventNotifier,
        LoggerFactory.getLogger(Ehcache.class + "-" + "CacheConfigurationListenerTest"));
    cache.init();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void testCacheConfigurationChangeFiresEvent () {
    Listener configurationListener = new Listener();
    List<CacheConfigurationChangeListener> cacheConfigurationChangeListeners
        = new ArrayList<CacheConfigurationChangeListener>();
    cacheConfigurationChangeListeners.add(configurationListener);
    this.runtimeConfiguration.addCacheConfigurationListener(cacheConfigurationChangeListeners);
    this.cache.getRuntimeConfiguration().updateResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
        .heap(10L, EntryUnit.ENTRIES).build());
    assertThat(configurationListener.eventSet.size(), is(1) );
  }

  @Test
  public void testRemovingCacheConfigurationListener() {
    Listener configurationListener = new Listener();
    List<CacheConfigurationChangeListener> cacheConfigurationChangeListeners
        = new ArrayList<CacheConfigurationChangeListener>();
    cacheConfigurationChangeListeners.add(configurationListener);
    this.runtimeConfiguration.addCacheConfigurationListener(cacheConfigurationChangeListeners);
    this.cache.getRuntimeConfiguration().updateResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
        .heap(20L, EntryUnit.ENTRIES).build());
    assertThat(configurationListener.eventSet.size(), is(1));
    this.runtimeConfiguration.removeCacheConfigurationListener(configurationListener);
    this.cache.getRuntimeConfiguration().updateResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
        .heap(5L, EntryUnit.ENTRIES).build());
    assertThat(configurationListener.eventSet.size(), is(1) );
  }

  private class Listener implements CacheConfigurationChangeListener {
    private final Set<CacheConfigurationChangeEvent> eventSet = new HashSet<CacheConfigurationChangeEvent>();

    @Override
    public void cacheConfigurationChange(CacheConfigurationChangeEvent event) {
      this.eventSet.add(event);
      Logger logger = LoggerFactory.getLogger(Ehcache.class + "-" + "GettingStarted");
      logger.info("Setting size: "+event.getNewValue().toString());
    }
  }
}
