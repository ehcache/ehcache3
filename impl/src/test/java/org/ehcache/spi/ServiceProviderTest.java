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

package org.ehcache.spi;

import org.ehcache.internal.store.DefaultStoreProvider;
import org.ehcache.internal.store.disk.OffHeapDiskStore;
import org.ehcache.internal.store.heap.OnHeapStore;
import org.ehcache.internal.store.offheap.OffHeapStore;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.hamcrest.core.IsSame;
import org.junit.Test;

import static org.junit.Assert.assertThat;

/**
 * @author Alex Snaps
 */
public class ServiceProviderTest {

  @Test
  public void testSupportsMultipleAuthoritativeTierProviders() throws Exception {

    ServiceLocator serviceLocator = new ServiceLocator();

    DefaultStoreProvider storeProvider = new DefaultStoreProvider();
    OnHeapStore.Provider cachingTierProvider = new OnHeapStore.Provider();
    OffHeapStore.Provider authoritativeTierProvider = new OffHeapStore.Provider();
    OffHeapDiskStore.Provider diskStoreProvider = new OffHeapDiskStore.Provider();

    serviceLocator.addService(storeProvider, true);
    serviceLocator.addService(cachingTierProvider, true);
    serviceLocator.addService(authoritativeTierProvider, true);
    serviceLocator.addService(diskStoreProvider, true);

    serviceLocator.startAllServices();

    assertThat(serviceLocator.findService(Store.Provider.class),
        IsSame.<Store.Provider>sameInstance(storeProvider));
    assertThat(serviceLocator.findService(CachingTier.Provider.class),
        IsSame.<CachingTier.Provider>sameInstance(cachingTierProvider));
    assertThat(serviceLocator.findService(AuthoritativeTier.Provider.class),
        IsSame.<AuthoritativeTier.Provider>sameInstance(authoritativeTierProvider));
    assertThat(serviceLocator.findService(diskStoreProvider.getClass()),
        IsSame.sameInstance(diskStoreProvider));
  }
}
