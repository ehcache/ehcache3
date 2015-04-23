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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.spi.LifeCyclable;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.Store.Configuration;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Test;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class UserManagedCacheTest {
  
  @Test
  public void testUserManagedCacheDelegatesLifecycleCallsToStore() throws Exception {
    final Store store = mock(Store.class);
    Store.Provider storeProvider = spy(new TestStoreProvider(store));
    ServiceLocator locator = new ServiceLocator(storeProvider);
    
    Ehcache ehcache = (Ehcache) UserManagedCacheBuilder.newUserManagedCacheBuilder(Object.class, Object.class, LoggerFactory.getLogger(Ehcache.class + "-" + "UserManagedCacheTest")).build(locator);
    final LifeCyclable mock = mock(LifeCyclable.class);
    ehcache.addHook(mock);
    ehcache.init();
    verify(mock).init();
    ehcache.close();
    verify(mock).close();
    ehcache.toMaintenance();
    verify(store).maintenance();
  }
  
  @Test
  public void testUserManagedEhcacheFailingTransitionGoesToLowestStatus() throws Exception {
    final Store store = mock(Store.class);
    Store.Provider storeProvider = spy(new TestStoreProvider(store));
    ServiceLocator locator = new ServiceLocator(storeProvider);
    Ehcache ehcache = (Ehcache) UserManagedCacheBuilder.newUserManagedCacheBuilder(Object.class, Object.class, LoggerFactory.getLogger(Ehcache.class + "-" + "UserManagedCacheTest")).build(locator);
    final LifeCyclable mock = mock(LifeCyclable.class);
    ehcache.addHook(mock);
    doThrow(new Exception()).when(mock).init();
    try {
      ehcache.init();
      fail();
    } catch (StateTransitionException e) {
      assertThat(ehcache.getStatus(), is(Status.UNINITIALIZED));
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

    doThrow(new RuntimeException()).when(store).maintenance();
    try {
      ehcache.toMaintenance();
      fail();
    } catch (StateTransitionException e) {
      assertThat(ehcache.getStatus(), is(Status.UNINITIALIZED));
    }

    reset(store);
    reset(mock);
    ehcache.toMaintenance();
    assertThat(ehcache.getStatus(), is(Status.MAINTENANCE));
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
    public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
    }

    @Override
    public void stop() {
    }

    @Override
    public <K, V> Store<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return store;
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
//      resource.close();
    }

    @Override
    public void initStore(Store<?, ?> resource) {

    }

  }
}
