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

import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEventNotificationServiceImpl;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.hamcrest.CoreMatchers;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    Set<EventType> eventTypes = new HashSet<EventType>();
    eventTypes.add(EventType.CREATED);
    eventTypes.add(EventType.REMOVED);
    eventTypes.add(EventType.UPDATED);
    ehcache.getRuntimeConfiguration().registerCacheEventListener(cacheEventListener, EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, eventTypes);
  }

  protected Ehcache<String, String> getEhcache(String name) {
    return getEhcache(null, name);
  }

  protected Ehcache<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter, String name) {
    ExecutorService orderedExecutor = Executors.newSingleThreadExecutor();
    ExecutorService unorderedExecutor = Executors.newCachedThreadPool();
    cacheEventNotificationService = new CacheEventNotificationServiceImpl<String, String>(orderedExecutor, unorderedExecutor, store);
    RuntimeConfiguration<String, String> runtimeConfiguration = new RuntimeConfiguration<String, String>(CACHE_CONFIGURATION, cacheEventNotificationService);
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
