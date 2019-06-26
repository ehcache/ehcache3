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

package org.ehcache.core.spi;

import org.ehcache.core.spi.service.CacheManagerProviderService;
import org.ehcache.core.spi.service.DiskResourceService;
import org.ehcache.impl.internal.store.disk.OffHeapDiskStore;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.offheap.OffHeapStore;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.hamcrest.core.IsCollectionContaining;
import org.hamcrest.core.IsSame;
import org.junit.Test;
import org.mockito.Answers;

import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 *
 */
public class ServiceProviderTest {

  @Test
  public void testSupportsMultipleAuthoritativeTierProviders() throws Exception {

    ServiceLocator.DependencySet dependencySet = dependencySet();

    OnHeapStore.Provider cachingTierProvider = new OnHeapStore.Provider();
    OffHeapStore.Provider authoritativeTierProvider = new OffHeapStore.Provider();
    OffHeapDiskStore.Provider diskStoreProvider = new OffHeapDiskStore.Provider();

    dependencySet.with(cachingTierProvider);
    dependencySet.with(authoritativeTierProvider);
    dependencySet.with(diskStoreProvider);
    dependencySet.with(mock(DiskResourceService.class));
    dependencySet.with(mock(CacheManagerProviderService.class, Answers.RETURNS_DEEP_STUBS));

    ServiceLocator serviceLocator = dependencySet.build();
    serviceLocator.startAllServices();

    assertThat(serviceLocator.getServicesOfType(CachingTier.Provider.class),
      IsCollectionContaining.<CachingTier.Provider>hasItem(IsSame.<CachingTier.Provider>sameInstance(cachingTierProvider)));
    assertThat(serviceLocator.getServicesOfType(AuthoritativeTier.Provider.class),
      IsCollectionContaining.<AuthoritativeTier.Provider>hasItem(IsSame.<AuthoritativeTier.Provider>sameInstance(authoritativeTierProvider)));
    assertThat(serviceLocator.getServicesOfType(OffHeapDiskStore.Provider.class),
      IsCollectionContaining.<OffHeapDiskStore.Provider>hasItem(IsSame.<OffHeapDiskStore.Provider>sameInstance(diskStoreProvider)));
  }
}
