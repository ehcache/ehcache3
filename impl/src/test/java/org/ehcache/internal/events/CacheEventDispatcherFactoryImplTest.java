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

package org.ehcache.internal.events;

import org.ehcache.config.event.DefaultCacheEventDispatcherConfiguration;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ExecutionService;
import org.junit.Test;

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
    ServiceProvider serviceProvider = mock(ServiceProvider.class);
    when(serviceProvider.getService(ExecutionService.class)).thenReturn(mock(ExecutionService.class));
    CacheEventDispatcherFactoryImpl factory = new CacheEventDispatcherFactoryImpl();
    factory.start(serviceProvider);
    DefaultCacheEventDispatcherConfiguration config = spy(new DefaultCacheEventDispatcherConfiguration("aName"));
    factory.createCacheEventDispatcher(mock(Store.class), config);
    verify(config).getThreadPoolAlias();
  }

}