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
import org.ehcache.config.ResourceType;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.config.ResourcePoolsHelper;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.core.spi.store.Store;
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.spi.LifeCycled;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class UserManagedCacheTest {

  @Test
  public void testUserManagedCacheDelegatesLifecycleCallsToStore() throws Exception {
    final Store store = mock(Store.class);
    CacheConfiguration<Object, Object> config = new BaseCacheConfiguration<Object, Object>(Object.class, Object.class, null, null,
        null, ResourcePoolsHelper.createHeapOnlyPools());
    EhcacheWithLoaderWriter ehcache = new EhcacheWithLoaderWriter(config, store, mock(CacheEventDispatcher.class), LoggerFactory.getLogger("testUserManagedCacheDelegatesLifecycleCallsToStore"));
    final LifeCycled mock = mock(LifeCycled.class);
    ehcache.addHook(mock);
    ehcache.init();
    verify(mock).init();
    ehcache.close();
    verify(mock).close();
  }

  @Test
  public void testUserManagedEhcacheFailingTransitionGoesToLowestStatus() throws Exception {
    final Store store = mock(Store.class);
    Store.Provider storeProvider = spy(new TestStoreProvider(store));
    ServiceLocator locator = new ServiceLocator(storeProvider);
    CacheConfiguration<Object, Object> config = new BaseCacheConfiguration<Object, Object>(Object.class, Object.class, null, null, null, ResourcePoolsHelper.createHeapOnlyPools());
    EhcacheWithLoaderWriter ehcache = new EhcacheWithLoaderWriter(config, store, mock(CacheEventDispatcher.class), LoggerFactory.getLogger("testUserManagedEhcacheFailingTransitionGoesToLowestStatus"));
    final LifeCycled mock = mock(LifeCycled.class);
    ehcache.addHook(mock);
    doThrow(new Exception()).when(mock).init();
    try {
      ehcache.init();
      fail();
    } catch (StateTransitionException e) {
      assertThat(ehcache.getStatus(), CoreMatchers.is(Status.UNINITIALIZED));
    }

    reset(mock);
    ehcache.init();
    assertThat(ehcache.getStatus(), is(Status.AVAILABLE));
    doThrow(new Exception()).when(mock).close();
    try {
      ehcache.close();
      fail();
    } catch (StateTransitionException e) {
      assertThat(ehcache.getStatus(), is(Status.UNINITIALIZED));
    }
  }

  private class TestStoreProvider implements Store.Provider {
    private Store store;

    public TestStoreProvider(Store store) {
      this.store = store;
    }

    @Override
    public void start(ServiceProvider<Service> serviceProvider) {
    }

    @Override
    public void stop() {
    }

    @Override
    public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return store;
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
//      resource.close();
    }

    @Override
    public void initStore(Store<?, ?> resource) {

    }

    @Override
    public int rank(Set<ResourceType> resourceTypes, Collection<ServiceConfiguration<?>> serviceConfigs) {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

  }
}
