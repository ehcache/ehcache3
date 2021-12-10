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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.impl.internal.util.UnmatchedResourceType;
import org.ehcache.impl.internal.store.offheap.portability.AssertingOffHeapValueHolderPortability;
import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Test;
import org.terracotta.statistics.StatisticsManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static java.util.Collections.EMPTY_LIST;
import static org.ehcache.impl.internal.spi.TestServiceProvider.providerContaining;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class OffHeapStoreTest extends AbstractOffHeapStoreTest {

  @Override
  protected OffHeapStore<String, String> createAndInitStore(TimeSource timeSource, ExpiryPolicy<? super String, ? super String> expiry) {
    try {
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining());
      ClassLoader classLoader = getClass().getClassLoader();
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, classLoader);
      Serializer<String> valueSerializer = serializationProvider.createValueSerializer(String.class, classLoader);
      StoreConfigurationImpl<String, String> storeConfiguration = new StoreConfigurationImpl<>(String.class, String.class,
        null, classLoader, expiry, null, 0, keySerializer, valueSerializer);
      OffHeapStore<String, String> offHeapStore = new OffHeapStore<String, String>(storeConfiguration, timeSource, new TestStoreEventDispatcher<>(), MemoryUnit.MB
        .toBytes(1), new DefaultStatisticsService()) {
        @Override
        protected OffHeapValueHolderPortability<String> createValuePortability(Serializer<String> serializer) {
          return new AssertingOffHeapValueHolderPortability<>(serializer);
        }
      };
      OffHeapStore.Provider.init(offHeapStore);
      return offHeapStore;
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  protected OffHeapStore<String, byte[]> createAndInitStore(TimeSource timeSource, ExpiryPolicy<? super String, ? super byte[]> expiry, EvictionAdvisor<? super String, ? super byte[]> evictionAdvisor) {
    try {
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining());
      ClassLoader classLoader = getClass().getClassLoader();
      Serializer<String> keySerializer = serializationProvider.createValueSerializer(String.class, classLoader);
      Serializer<byte[]> valueSerializer = serializationProvider.createValueSerializer(byte[].class, classLoader);
      StoreConfigurationImpl<String, byte[]> storeConfiguration = new StoreConfigurationImpl<>(String.class, byte[].class,
        evictionAdvisor, getClass().getClassLoader(), expiry, null, 0, keySerializer, valueSerializer);
      OffHeapStore<String, byte[]> offHeapStore = new OffHeapStore<String, byte[]>(storeConfiguration, timeSource, new TestStoreEventDispatcher<>(), MemoryUnit.MB
        .toBytes(1), new DefaultStatisticsService()) {
        @Override
        protected OffHeapValueHolderPortability<byte[]> createValuePortability(Serializer<byte[]> serializer) {
          return new AssertingOffHeapValueHolderPortability<>(serializer);
        }
      };
      OffHeapStore.Provider.init(offHeapStore);
      return offHeapStore;
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRankAuthority() throws Exception {
    OffHeapStore.Provider provider = new OffHeapStore.Provider();

    assertThat(provider.rankAuthority(ResourceType.Core.OFFHEAP, EMPTY_LIST), is(1));
    assertThat(provider.rankAuthority(new UnmatchedResourceType(), EMPTY_LIST), is(0));
  }

  @Test
  public void testRank() throws Exception {
    OffHeapStore.Provider provider = new OffHeapStore.Provider();

    assertRank(provider, 1, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK);
    assertRank(provider, 0, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);

    assertRank(provider, 0, new UnmatchedResourceType());
    assertRank(provider, 0, ResourceType.Core.OFFHEAP, new UnmatchedResourceType());
  }

  private void assertRank(final Store.Provider provider, final int expectedRank, final ResourceType<?>... resources) {
    assertThat(provider.rank(
      new HashSet<>(Arrays.asList(resources)),
        Collections.<ServiceConfiguration<?, ?>>emptyList()),
        is(expectedRank));
  }

  @Override
  protected void destroyStore(AbstractOffHeapStore<?, ?> store) {
    OffHeapStore.Provider.close((OffHeapStore<?, ?>) store);
    StatisticsManager.nodeFor(store).clean();
  }
}
