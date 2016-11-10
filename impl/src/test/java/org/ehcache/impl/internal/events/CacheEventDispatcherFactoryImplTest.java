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

package org.ehcache.impl.internal.events;

import org.ehcache.impl.config.event.DefaultCacheEventDispatcherConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.impl.events.CacheEventDispatcherImpl;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.service.ExecutionService;
import org.ehcache.spi.service.Service;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * CacheEventDispatcherFactoryImplTest
 */
public class CacheEventDispatcherFactoryImplTest {

  @Test
  public void testConfigurationOfThreadPoolAlias() {
    @SuppressWarnings("unchecked")
    ServiceProvider<Service> serviceProvider = mock(ServiceProvider.class);
    when(serviceProvider.getService(ExecutionService.class)).thenReturn(mock(ExecutionService.class));
    CacheEventDispatcherFactoryImpl factory = new CacheEventDispatcherFactoryImpl();
    factory.start(serviceProvider);
    DefaultCacheEventDispatcherConfiguration config = spy(new DefaultCacheEventDispatcherConfiguration("aName"));
    @SuppressWarnings("unchecked")
    Store<Object, Object> store = mock(Store.class);
    factory.createCacheEventDispatcher(store, config);
    verify(config).getThreadPoolAlias();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateCacheEventDispatcherReturnsDisabledDispatcherWhenNoThreadPool() throws Exception {
    ServiceProvider<Service> serviceProvider = mock(ServiceProvider.class);
    ExecutionService executionService = mock(ExecutionService.class);
    when(serviceProvider.getService(ExecutionService.class)).thenReturn(executionService);
    when(executionService.getOrderedExecutor(eq("myAlias"), any(BlockingQueue.class))).thenThrow(IllegalArgumentException.class);
    when(executionService.getUnorderedExecutor(eq("myAlias"), any(BlockingQueue.class))).thenThrow(IllegalArgumentException.class);

    CacheEventDispatcherFactoryImpl cacheEventDispatcherFactory = new CacheEventDispatcherFactoryImpl();
    cacheEventDispatcherFactory.start(serviceProvider);

    @SuppressWarnings("unchecked")
    Store<Object, Object> store = mock(Store.class);
    try {
      cacheEventDispatcherFactory.createCacheEventDispatcher(store, new DefaultCacheEventDispatcherConfiguration("myAlias"));
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateCacheEventReturnsDisabledDispatcherWhenThreadPoolFound() throws Exception {
    ServiceProvider<Service> serviceProvider = mock(ServiceProvider.class);
    ExecutionService executionService = mock(ExecutionService.class);
    when(serviceProvider.getService(ExecutionService.class)).thenReturn(executionService);
    when(executionService.getOrderedExecutor(eq("myAlias"), any(BlockingQueue.class))).thenReturn(mock(ExecutorService.class));
    when(executionService.getUnorderedExecutor(eq("myAlias"), any(BlockingQueue.class))).thenReturn(mock(ExecutorService.class));

    CacheEventDispatcherFactoryImpl cacheEventDispatcherFactory = new CacheEventDispatcherFactoryImpl();
    cacheEventDispatcherFactory.start(serviceProvider);

    Store<Object, Object> store = mock(Store.class);
    CacheEventDispatcher dispatcher = cacheEventDispatcherFactory.createCacheEventDispatcher(store, new DefaultCacheEventDispatcherConfiguration("myAlias"));
    assertThat(dispatcher, instanceOf(CacheEventDispatcherImpl.class));
  }
}
