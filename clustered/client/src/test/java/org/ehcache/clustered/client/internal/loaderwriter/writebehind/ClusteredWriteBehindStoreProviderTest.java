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
package org.ehcache.clustered.client.internal.loaderwriter.writebehind;

import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.internal.store.ClusteredStoreProviderTest;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.service.DiskResourceService;
import org.ehcache.impl.internal.store.disk.OffHeapDiskStore;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.offheap.OffHeapStore;
import org.ehcache.impl.internal.store.tiering.TieredStore;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class ClusteredWriteBehindStoreProviderTest {

  private final CacheLoaderWriterConfiguration<?> cacheLoaderWriterConfiguration = mock(CacheLoaderWriterConfiguration.class);
  private final WriteBehindConfiguration<?> writeBehindConfiguration = mock(WriteBehindConfiguration.class);

  @Test
  public void testRank() {
    ClusteredWriteBehindStore.Provider provider = new ClusteredWriteBehindStore.Provider();
    ServiceLocator serviceLocator = dependencySet()
      .with(new TieredStore.Provider())
      .with(new OnHeapStore.Provider())
      .with(new OffHeapStore.Provider())
      .with(mock(DiskResourceService.class))
      .with(new OffHeapDiskStore.Provider())
      .with(mock(ClusteringService.class)).build();
    provider.start(serviceLocator);

    assertThat(provider.rank(new HashSet<>(Collections.singletonList(ClusteredResourceType.Types.DEDICATED)),
                             Arrays.asList(cacheLoaderWriterConfiguration, writeBehindConfiguration)), is(3));
    assertThat(provider.rank(new HashSet<>(Collections.singletonList(ClusteredResourceType.Types.DEDICATED)),
                             Collections.singletonList(cacheLoaderWriterConfiguration)), is(0));
    assertThat(provider.rank(new HashSet<>(Collections.singletonList(new ClusteredStoreProviderTest.UnmatchedResourceType())),
                             Arrays.asList(cacheLoaderWriterConfiguration, writeBehindConfiguration)), is(0));
  }

  @Test
  public void testAuthoritativeRank() {
    ClusteredWriteBehindStore.Provider provider = new ClusteredWriteBehindStore.Provider();
    ServiceLocator serviceLocator = dependencySet().with(mock(ClusteringService.class)).build();
    provider.start(serviceLocator);

    assertThat(provider.rankAuthority(ClusteredResourceType.Types.DEDICATED,
                                      Arrays.asList(cacheLoaderWriterConfiguration, writeBehindConfiguration)),
               is(3));
    assertThat(provider.rankAuthority(ClusteredResourceType.Types.DEDICATED,
                                      Collections.singletonList(writeBehindConfiguration)),
               is(0));
    assertThat(provider.rankAuthority(new ClusteredStoreProviderTest.UnmatchedResourceType(), Arrays.asList(cacheLoaderWriterConfiguration,
                                                                                                            writeBehindConfiguration)),
               is(0));
  }
}
