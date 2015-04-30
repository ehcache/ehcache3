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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEventNotificationServiceImpl;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.util.IsRemoved;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

/**
 * Provides testing of events for basic REMOVE(key, value) operations.
 *
 */
public class EhcacheBasicRemoveValueEventsTest extends EhcacheBasicCrudBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  @Mock
  protected CacheEventListener<String,String> cacheEventListener;

  protected CacheEventNotificationService<String,String> cacheEventNotificationService;

  protected IsRemoved isRemoved = new IsRemoved();

  @Test
  public void testRemoveNullNull() {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.remove(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener,never()).onEvent(Matchers.<CacheEvent<String, String>>any());
    ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
  }

  @Test
  public void testRemoveKeyNull() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    try {
      ehcache.remove("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener,never()).onEvent(Matchers.<CacheEvent<String, String>>any());
    ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
  }

  @Test
  public void testRemoveNullValue() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache(null);
    try {
      ehcache.remove(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener,never()).onEvent(Matchers.<CacheEvent<String, String>>any());
    ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
  }

  @Test
  public void testRemoveValueNoStoreEntryNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    final Ehcache<String, String> ehcache = this.getEhcache(null);
    assertFalse(ehcache.remove("key", "value"));
    verify(cacheEventListener,never()).onEvent(Matchers.<CacheEvent<String, String>>any());
    try {
      ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
      fail();
    } catch (UnsupportedOperationException e){
      //expected
    }
  }

  @Test
  public void testRemoveValueEqualStoreEntryNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.singletonMap("key", "value"));
    this.store = spy(fakeStore);
    final Ehcache<String, String> ehcache = this.getEhcache(null);
    assertTrue(ehcache.remove("key", "value"));
    verify(cacheEventListener,times(1)).onEvent(argThat(isRemoved));
    try {
      ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
      fail();
    } catch (UnsupportedOperationException e){
      //expected
    }
  }

  @Test
  public void testRemoveValueNoStoreEntryCacheAccessExceptionNoCacheLoaderWriter() throws Exception {
    final FakeStore fakeStore = new FakeStore(Collections.<String, String>emptyMap());
    this.store = spy(fakeStore);
    doThrow(new CacheAccessException("")).when(this.store).compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    final Ehcache<String, String> ehcache = this.getEhcache(null);
    ehcache.remove("key", "value");
    verify(cacheEventListener,never()).onEvent(Matchers.<CacheEvent<String, String>>any());
    try {
      ehcache.getRuntimeConfiguration().deregisterCacheEventListener(cacheEventListener);
      fail();
    } catch (UnsupportedOperationException e){
      //expected
    }
  }

  private Ehcache<String, String> getEhcache(final CacheLoaderWriter<String, String> cacheLoaderWriter) {
    ExecutorService orderedExecutor = Executors.newSingleThreadExecutor();
    ExecutorService unorderedExecutor = Executors.newCachedThreadPool();
    cacheEventNotificationService = new CacheEventNotificationServiceImpl<String, String>(orderedExecutor, unorderedExecutor);
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store,
            cacheLoaderWriter,cacheEventNotificationService,null,
            LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBasicRemoveEventsTest"));
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), CoreMatchers.is(Status.AVAILABLE));
    super.registerCacheEventListener(ehcache, cacheEventListener);
    return ehcache;
  }
}