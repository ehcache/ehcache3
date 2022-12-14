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

import org.ehcache.Status;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.config.ResourcePoolsHelper;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.spi.store.Store;
import org.ehcache.StateTransitionException;
import org.ehcache.core.spi.LifeCycled;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class UserManagedCacheTest {

  @Test
  public void testUserManagedCacheDelegatesLifecycleCallsToStore() throws Exception {
    final Store store = mock(Store.class);
    CacheConfiguration<Object, Object> config = new BaseCacheConfiguration<>(Object.class, Object.class, null, null,
      null, ResourcePoolsHelper.createHeapOnlyPools());
    Ehcache ehcache = new Ehcache(config, store, mock(ResilienceStrategy.class), mock(CacheEventDispatcher.class), LoggerFactory.getLogger(Ehcache.class + "testUserManagedCacheDelegatesLifecycleCallsToStore"));
    assertCacheDelegatesLifecycleCallsToStore(ehcache);

    Ehcache ehcacheWithLoaderWriter = new Ehcache(config, store, mock(ResilienceStrategy.class),
        mock(CacheEventDispatcher.class), LoggerFactory.getLogger(Ehcache.class + "testUserManagedCacheDelegatesLifecycleCallsToStore"),
            mock(CacheLoaderWriter.class));
    assertCacheDelegatesLifecycleCallsToStore(ehcacheWithLoaderWriter);
  }

  private void assertCacheDelegatesLifecycleCallsToStore(InternalCache cache) throws Exception {
    final LifeCycled mock = mock(LifeCycled.class);
    cache.addHook(mock);
    cache.init();
    verify(mock).init();
    cache.close();
    verify(mock).close();
  }

  @Test
  public void testUserManagedEhcacheFailingTransitionGoesToLowestStatus() throws Exception {
    final Store store = mock(Store.class);
    CacheConfiguration<Object, Object> config = new BaseCacheConfiguration<>(Object.class, Object.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    Ehcache ehcache = new Ehcache(config, store, mock(ResilienceStrategy.class), mock(CacheEventDispatcher.class), LoggerFactory.getLogger(Ehcache.class + "testUserManagedEhcacheFailingTransitionGoesToLowestStatus"));
    assertFailingTransitionGoesToLowestStatus(ehcache);
    Ehcache ehcacheWithLoaderWriter = new Ehcache(config, store, mock(ResilienceStrategy.class),
        mock(CacheEventDispatcher.class), LoggerFactory.getLogger(Ehcache.class + "testUserManagedCacheDelegatesLifecycleCallsToStore"), mock(CacheLoaderWriter.class));
    assertFailingTransitionGoesToLowestStatus(ehcacheWithLoaderWriter);
  }

  private void assertFailingTransitionGoesToLowestStatus(InternalCache cache) throws Exception {
    final LifeCycled mock = mock(LifeCycled.class);
    cache.addHook(mock);
    doThrow(new Exception()).when(mock).init();
    try {
      cache.init();
      fail();
    } catch (StateTransitionException e) {
      assertThat(cache.getStatus(), CoreMatchers.is(Status.UNINITIALIZED));
    }

    reset(mock);
    cache.init();
    assertThat(cache.getStatus(), is(Status.AVAILABLE));
    doThrow(new Exception()).when(mock).close();
    try {
      cache.close();
      fail();
    } catch (StateTransitionException e) {
      assertThat(cache.getStatus(), is(Status.UNINITIALIZED));
    }

  }

}
