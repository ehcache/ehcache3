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
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author rism
 */
public class CacheConfigurationListenerTest {
  private Store<Object, Object> store;
  private CacheEventNotificationService<Object, Object> eventNotifier;
  private InternalRuntimeConfigurationImpl<Object, Object> internalRuntimeConfiguration;
  private RuntimeConfiguration<Object, Object> runtimeConfiguration;
  private CacheConfiguration<Object, Object> config;
  private Ehcache<Object, Object> cache;

  @SuppressWarnings({ "unchecked"})
  @Before
  public void setUp() throws Exception {
    this.store = mock(Store.class);
    this.eventNotifier = mock(CacheEventNotificationService.class);
    CacheLoaderWriter<Object, Object> loaderWriter = mock(CacheLoaderWriter.class);
    this.config = newCacheConfigurationBuilder().buildConfig(Object.class, Object.class);
    this.internalRuntimeConfiguration = new InternalRuntimeConfigurationImpl<Object, Object>(this.config,
        this.store, this.eventNotifier);
    this.runtimeConfiguration = new RuntimeConfiguration<Object, Object>(this.config, this.internalRuntimeConfiguration);
    this.cache = new Ehcache<Object, Object>(runtimeConfiguration, store, loaderWriter, eventNotifier, null,
        LoggerFactory.getLogger(Ehcache.class + "-" + "CacheConfigurationListenerTest"));
    cache.init();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void testCacheConfigurationChangeFiresEvent () {
    CacheConfigurationListener configurationListener = mock(CacheConfigurationListener.class);
    this.internalRuntimeConfiguration.addCacheConfigurationListener(configurationListener);
    this.cache.getRuntimeConfiguration().setCapacityConstraint(2L);
    verify(configurationListener, times(1)).cacheConfigurationChange(any(CacheConfigurationChangeEvent.class));
  }

  @Test
  public void testRemovingCacheConfigurationListener() {
    CacheConfigurationListener configurationListener = mock(CacheConfigurationListener.class);
    this.internalRuntimeConfiguration.addCacheConfigurationListener(configurationListener);
    this.cache.getRuntimeConfiguration().setCapacityConstraint(2L);
    verify(configurationListener, times(1)).cacheConfigurationChange(any(CacheConfigurationChangeEvent.class));
    this.internalRuntimeConfiguration.removeCacheConfigurationListener(configurationListener);
    this.cache.getRuntimeConfiguration().setCapacityConstraint(5L);
    verify(configurationListener, times(1)).cacheConfigurationChange(any(CacheConfigurationChangeEvent.class));
  }
}
