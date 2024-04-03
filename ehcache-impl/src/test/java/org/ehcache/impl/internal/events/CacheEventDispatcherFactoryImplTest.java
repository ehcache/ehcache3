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

import org.ehcache.core.spi.ServiceLocatorUtils;
import org.ehcache.impl.config.event.DefaultCacheEventDispatcherConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.impl.events.CacheEventDispatcherImpl;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.service.ExecutionService;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

import static org.ehcache.core.spi.ServiceLocatorUtils.withServiceLocator;
import static org.ehcache.test.MockitoUtil.uncheckedGenericMock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * CacheEventDispatcherFactoryImplTest
 */
public class CacheEventDispatcherFactoryImplTest {

  @Test
  public void testConfigurationOfThreadPoolAlias() throws Exception {
    withServiceLocator(new CacheEventDispatcherFactoryImpl(), provider -> {
      DefaultCacheEventDispatcherConfiguration config = spy(new DefaultCacheEventDispatcherConfiguration("aName"));
      @SuppressWarnings("unchecked")
      Store<Object, Object> store = mock(Store.class);
      provider.createCacheEventDispatcher(store, config);
      verify(config).getThreadPoolAlias();
    });
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateCacheEventDispatcherReturnsDisabledDispatcherWhenNoThreadPool() throws Exception {
    ExecutionService executionService = mock(ExecutionService.class);
    when(executionService.getOrderedExecutor(eq("myAlias"), any(BlockingQueue.class))).thenThrow(IllegalArgumentException.class);
    when(executionService.getUnorderedExecutor(eq("myAlias"), any(BlockingQueue.class))).thenThrow(IllegalArgumentException.class);

    withServiceLocator(new CacheEventDispatcherFactoryImpl(), deps -> deps.with(executionService), cacheEventDispatcherFactory -> {
      Store<Object, Object> store = uncheckedGenericMock(Store.class);
      try {
        cacheEventDispatcherFactory.createCacheEventDispatcher(store, new DefaultCacheEventDispatcherConfiguration("myAlias"));
        fail("expected IllegalArgumentException");
      } catch (IllegalArgumentException iae) {
        // expected
      }
    });
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateCacheEventReturnsDisabledDispatcherWhenThreadPoolFound() throws Exception {
    ExecutionService executionService = mock(ExecutionService.class);
    when(executionService.getOrderedExecutor(eq("myAlias"), any(BlockingQueue.class))).thenReturn(mock(ExecutorService.class));
    when(executionService.getUnorderedExecutor(eq("myAlias"), any(BlockingQueue.class))).thenReturn(mock(ExecutorService.class));

    withServiceLocator(new CacheEventDispatcherFactoryImpl(), deps -> deps.with(executionService), cacheEventDispatcherFactory -> {
      Store<Object, Object> store = mock(Store.class);
      CacheEventDispatcher<Object, Object> dispatcher = cacheEventDispatcherFactory.createCacheEventDispatcher(store, new DefaultCacheEventDispatcherConfiguration("myAlias"));
      assertThat(dispatcher, instanceOf(CacheEventDispatcherImpl.class));
    });
  }
}
