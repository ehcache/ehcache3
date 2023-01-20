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

package org.ehcache.impl.internal.store.disk;

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.core.internal.store.StoreConfigurationImpl;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.exceptions.StoreAccessException;
import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.expiry.Expiry;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.impl.internal.executor.OnDemandExecutionService;
import org.ehcache.impl.internal.persistence.TestLocalPersistenceService;
import org.ehcache.impl.internal.store.offheap.AbstractOffHeapStore;
import org.ehcache.impl.internal.store.offheap.AbstractOffHeapStoreTest;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.core.spi.service.LocalPersistenceService.PersistenceSpaceIdentifier;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.ehcache.expiry.Expirations.noExpiration;
import static org.ehcache.impl.internal.spi.TestServiceProvider.providerContaining;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OffHeapDiskStoreTest extends AbstractOffHeapStoreTest {

  @Rule
  public final TestLocalPersistenceService persistenceService = new TestLocalPersistenceService();

  @Test
  public void testRecovery() throws StoreAccessException, IOException {
    OffHeapDiskStore<String, String> offHeapDiskStore = createAndInitStore(SystemTimeSource.INSTANCE, noExpiration());
    try {
      offHeapDiskStore.put("key1", "value1");
      assertThat(offHeapDiskStore.get("key1"), notNullValue());

      OffHeapDiskStore.Provider.close(offHeapDiskStore);

      OffHeapDiskStore.Provider.init(offHeapDiskStore);
      assertThat(offHeapDiskStore.get("key1"), notNullValue());
    } finally {
      destroyStore(offHeapDiskStore);
    }
  }

  @Test
  public void testRecoveryFailureWhenValueTypeChangesToIncompatibleClass() throws Exception {
    OffHeapDiskStore.Provider provider = new OffHeapDiskStore.Provider();
    ServiceLocator serviceLocator = new ServiceLocator();
    serviceLocator.addService(persistenceService);
    serviceLocator.addService(provider);
    serviceLocator.startAllServices();

    PersistenceSpaceIdentifier space = persistenceService.getOrCreatePersistenceSpace("cache");

    {
      Store.Configuration<Long, String> storeConfig1 = mock(Store.Configuration.class);
      when(storeConfig1.getKeyType()).thenReturn(Long.class);
      when(storeConfig1.getValueType()).thenReturn(String.class);
      when(storeConfig1.getResourcePools()).thenReturn(ResourcePoolsBuilder.newResourcePoolsBuilder()
          .disk(10, MemoryUnit.MB)
          .build());
      when(storeConfig1.getOrderedEventParallelism()).thenReturn(1);

      OffHeapDiskStore<Long, String> offHeapDiskStore1 = provider.createStore(storeConfig1, space);
      provider.initStore(offHeapDiskStore1);

      destroyStore(offHeapDiskStore1);
    }

    {
      Store.Configuration<Long, Serializable> storeConfig2 = mock(Store.Configuration.class);
      when(storeConfig2.getKeyType()).thenReturn(Long.class);
      when(storeConfig2.getValueType()).thenReturn(Serializable.class);
      when(storeConfig2.getResourcePools()).thenReturn(ResourcePoolsBuilder.newResourcePoolsBuilder()
          .disk(10, MemoryUnit.MB)
          .build());
      when(storeConfig2.getOrderedEventParallelism()).thenReturn(1);
      when(storeConfig2.getClassLoader()).thenReturn(ClassLoader.getSystemClassLoader());


      OffHeapDiskStore<Long, Serializable> offHeapDiskStore2 = provider.createStore(storeConfig2, space);
      try {
        provider.initStore(offHeapDiskStore2);
        fail("expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        // expected
      }

      destroyStore(offHeapDiskStore2);
    }
  }

  @Override
  protected OffHeapDiskStore<String, String> createAndInitStore(final TimeSource timeSource, final Expiry<? super String, ? super String> expiry) {
    try {
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining(persistenceService));
      ClassLoader classLoader = getClass().getClassLoader();
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, classLoader);
      Serializer<String> valueSerializer = serializationProvider.createValueSerializer(String.class, classLoader);
      StoreConfigurationImpl<String, String> storeConfiguration = new StoreConfigurationImpl<String, String>(String.class, String.class,
          null, classLoader, expiry, null, 0, keySerializer, valueSerializer);
      OffHeapDiskStore<String, String> offHeapStore = new OffHeapDiskStore<String, String>(
              getPersistenceContext(),
              new OnDemandExecutionService(), null, 1,
              storeConfiguration, timeSource,
              new TestStoreEventDispatcher<String, String>(),
              MemoryUnit.MB.toBytes(1));
      OffHeapDiskStore.Provider.init(offHeapStore);
      return offHeapStore;
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  protected OffHeapDiskStore<String, byte[]> createAndInitStore(TimeSource timeSource, Expiry<? super String, ? super byte[]> expiry, EvictionAdvisor<? super String, ? super byte[]> evictionAdvisor) {
    try {
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining(persistenceService));
      ClassLoader classLoader = getClass().getClassLoader();
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, classLoader);
      Serializer<byte[]> valueSerializer = serializationProvider.createValueSerializer(byte[].class, classLoader);
      StoreConfigurationImpl<String, byte[]> storeConfiguration = new StoreConfigurationImpl<String, byte[]>(String.class, byte[].class,
          evictionAdvisor, getClass().getClassLoader(), expiry, null, 0, keySerializer, valueSerializer);
      OffHeapDiskStore<String, byte[]> offHeapStore = new OffHeapDiskStore<String, byte[]>(
              getPersistenceContext(),
              new OnDemandExecutionService(), null, 1,
              storeConfiguration, timeSource,
              new TestStoreEventDispatcher<String, byte[]>(),
              MemoryUnit.MB.toBytes(1));
      OffHeapDiskStore.Provider.init(offHeapStore);
      return offHeapStore;
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  protected void destroyStore(AbstractOffHeapStore<?, ?> store) {
    try {
      OffHeapDiskStore.Provider.close((OffHeapDiskStore<?, ?>) store);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void testStoreInitFailsWithoutLocalPersistenceService() throws Exception {
    OffHeapDiskStore.Provider provider = new OffHeapDiskStore.Provider();
    ServiceLocator serviceLocator = new ServiceLocator();
    serviceLocator.addService(provider);
    serviceLocator.startAllServices();
    Store.Configuration<String, String> storeConfig = mock(Store.Configuration.class);
    when(storeConfig.getKeyType()).thenReturn(String.class);
    when(storeConfig.getValueType()).thenReturn(String.class);
    when(storeConfig.getResourcePools()).thenReturn(ResourcePoolsBuilder.newResourcePoolsBuilder()
        .disk(10, MemoryUnit.MB)
        .build());
    when(storeConfig.getOrderedEventParallelism()).thenReturn(1);
    try {
      provider.createStore(storeConfig);
      fail("IllegalStateException expected");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("No LocalPersistenceService could be found - did you configure it at the CacheManager level?"));
    }
  }

  @Test
  public void testRank() throws Exception {
    OffHeapDiskStore.Provider provider = new OffHeapDiskStore.Provider();

    assertRank(provider, 1, ResourceType.Core.DISK);
    assertRank(provider, 0, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);

    final ResourceType<ResourcePool> unmatchedResourceType = new ResourceType<ResourcePool>() {
      @Override
      public Class<ResourcePool> getResourcePoolClass() {
        return ResourcePool.class;
      }
      @Override
      public boolean isPersistable() {
        return true;
      }
      @Override
      public boolean requiresSerialization() {
        return true;
      }
    };
    assertRank(provider, 0, unmatchedResourceType);
    assertRank(provider, 0, ResourceType.Core.DISK, unmatchedResourceType);
  }

  private void assertRank(final Store.Provider provider, final int expectedRank, final ResourceType<?>... resources) {
    assertThat(provider.rank(
        new HashSet<ResourceType<?>>(Arrays.asList(resources)),
        Collections.<ServiceConfiguration<?>>emptyList()),
        is(expectedRank));
  }

  private FileBasedPersistenceContext getPersistenceContext() {
    try {
      PersistenceSpaceIdentifier space = persistenceService.getOrCreatePersistenceSpace("cache");
      return persistenceService.createPersistenceContextWithin(space, "store");
    } catch (CachePersistenceException e) {
      throw new AssertionError(e);
    }
  }

}