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

import java.util.EnumSet;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEventNotificationServiceImpl;
import org.ehcache.events.OrderedEventDispatcher;
import org.ehcache.events.UnorderedEventDispatcher;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.hamcrest.CoreMatchers;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.spy;

/**
 * EhcacheEventsTestBase
 */
public class EhcacheEventsTestBase extends EhcacheBasicCrudBase {

  @Mock
  protected CacheEventListener<String, String> cacheEventListener;

  protected CacheEventNotificationService<String, String> cacheEventNotificationService;

  protected <K, V> void registerCacheEventListener(Cache<K, V> ehcache, CacheEventListener<? super K, ? super V> cacheEventListener) {
    ehcache.getRuntimeConfiguration().registerCacheEventListener(cacheEventListener, EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, EnumSet.allOf(EventType.class));
  }

  protected Ehcache<String, String> getEhcache(String name) {
    return getEhcache(null, Expirations.noExpiration(), name);
  }

  protected Ehcache<String, String> getEhcache(Expiry<? super String, ? super String> expiry, String name) {
    return getEhcache(null, expiry, name);
  }
  
  protected Ehcache<String, String> getEhcache(CacheLoaderWriter<String, String> cacheLoaderWriter, String name) {
    return getEhcache(cacheLoaderWriter, Expirations.noExpiration(), name);
  }
  
  protected Ehcache<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter, Expiry<? super String, ? super String> expiry, String name) {
    CacheConfiguration<String, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder().withExpiry(expiry).buildConfig(String.class, String.class);
    ExecutorService orderedExecutor = Executors.newSingleThreadExecutor();
    ExecutorService unorderedExecutor = Executors.newCachedThreadPool();
    TimeSource timeSource = SystemTimeSource.INSTANCE;
    cacheEventNotificationService = new CacheEventNotificationServiceImpl<String, String>(store, new OrderedEventDispatcher<String, String>(orderedExecutor),
        new UnorderedEventDispatcher<String, String>(unorderedExecutor), timeSource);
    EhcacheRuntimeConfiguration<String, String> runtimeConfiguration = new EhcacheRuntimeConfiguration<String, String>(config, cacheEventNotificationService);
    runtimeConfiguration.addCacheConfigurationListener(cacheEventNotificationService.getConfigurationChangeListeners());
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(runtimeConfiguration, this.store,
        cacheLoaderWriter, cacheEventNotificationService,
        LoggerFactory.getLogger(Ehcache.class + "-" + name));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
    registerCacheEventListener(ehcache, cacheEventListener);
    return ehcache;
  }

  protected void buildStore(Map<String, String> originalStoreContent) {
    final FakeStore fakeStore = new FakeStore(originalStoreContent);
    this.store = spy(fakeStore);
  }

  protected void buildStore(Map<String, String> originalStoreContent, Set<String> failingKeys) {
    final FakeStore fakeStore = new FakeStore(originalStoreContent, failingKeys);
    this.store = spy(fakeStore);
  }
}
