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

package org.ehcache.core;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.config.ResourcePoolsHelper;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

/**
 * @author rism
 */
public class CacheConfigurationChangeListenerTest {
  private Store<Object, Object> store;
  private CacheEventDispatcher<Object, Object> eventNotifier;
  private EhcacheRuntimeConfiguration<Object, Object> runtimeConfiguration;
  private CacheConfiguration<Object, Object> config;
  private Ehcache<Object, Object> cache;

  @SuppressWarnings({ "unchecked"})
  @Before
  public void setUp() throws Exception {
    this.store = mock(Store.class);
    this.eventNotifier = mock(CacheEventDispatcher.class);
    ResilienceStrategy<Object, Object> resilienceStrategy = mock(ResilienceStrategy.class);
    CacheLoaderWriter<Object, Object> loaderWriter = mock(CacheLoaderWriter.class);
    this.config = new BaseCacheConfiguration<>(Object.class, Object.class, null, null, null, ResourcePoolsHelper.createHeapDiskPools(2, 10));
    this.cache = new Ehcache<>(config, store, resilienceStrategy, eventNotifier,
      LoggerFactory.getLogger(Ehcache.class + "-" + "CacheConfigurationListenerTest"), loaderWriter);
    cache.init();
    this.runtimeConfiguration = (EhcacheRuntimeConfiguration<Object, Object>)cache.getRuntimeConfiguration();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void testCacheConfigurationChangeFiresEvent () {
    Listener configurationListener = new Listener();
    List<CacheConfigurationChangeListener> cacheConfigurationChangeListeners
        = new ArrayList<>();
    cacheConfigurationChangeListeners.add(configurationListener);
    this.runtimeConfiguration.addCacheConfigurationListener(cacheConfigurationChangeListeners);
    this.cache.getRuntimeConfiguration().updateResourcePools(ResourcePoolsHelper.createHeapOnlyPools(10));
    assertThat(configurationListener.eventSet.size(), is(1) );
  }

  @Test
  public void testRemovingCacheConfigurationListener() {
    Listener configurationListener = new Listener();
    List<CacheConfigurationChangeListener> cacheConfigurationChangeListeners
        = new ArrayList<>();
    cacheConfigurationChangeListeners.add(configurationListener);
    this.runtimeConfiguration.addCacheConfigurationListener(cacheConfigurationChangeListeners);
    this.cache.getRuntimeConfiguration().updateResourcePools(ResourcePoolsHelper.createHeapOnlyPools(20));
    assertThat(configurationListener.eventSet.size(), is(1));
    this.runtimeConfiguration.removeCacheConfigurationListener(configurationListener);
    this.cache.getRuntimeConfiguration().updateResourcePools(ResourcePoolsHelper.createHeapOnlyPools(5));
    assertThat(configurationListener.eventSet.size(), is(1) );
  }

  private class Listener implements CacheConfigurationChangeListener {
    private final Set<CacheConfigurationChangeEvent> eventSet = new HashSet<>();

    @Override
    public void cacheConfigurationChange(CacheConfigurationChangeEvent event) {
      this.eventSet.add(event);
      Logger logger = LoggerFactory.getLogger(Ehcache.class + "-" + "GettingStarted");
      logger.info("Setting size: "+event.getNewValue().toString());
    }
  }
}
