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

package org.ehcache.internal.store.disk;

import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.persistence.TestLocalPersistenceService;
import org.ehcache.internal.store.offheap.AbstractOffHeapStore;
import org.ehcache.internal.store.offheap.AbstractOffHeapStoreTest;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.FileBasedPersistenceContext;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static org.ehcache.expiry.Expirations.noExpiration;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OffHeapDiskStoreTest extends AbstractOffHeapStoreTest {

  @Rule
  public final TestLocalPersistenceService persistenceService = new TestLocalPersistenceService();
  
  @Ignore
  public void testEvictionVeto() {}
  
  @Test
  public void testRecovery() throws CacheAccessException, IOException {
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
  
  @Override
  protected OffHeapDiskStore<String, String> createAndInitStore(final TimeSource timeSource, final Expiry<? super String, ? super String> expiry) {
    SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(null);
    ClassLoader classLoader = getClass().getClassLoader();
    Serializer<String> serializer = serializationProvider.createValueSerializer(String.class, classLoader);
    StoreConfigurationImpl<String, String> storeConfiguration = new StoreConfigurationImpl<String, String>(String.class, String.class, null, null, classLoader, expiry, null);
    OffHeapDiskStore<String, String> offHeapStore = new OffHeapDiskStore<String, String>(getPersistenceContext(), storeConfiguration, serializer, serializer, timeSource, MemoryUnit.MB.toBytes(1));
    OffHeapDiskStore.Provider.init(offHeapStore);
    return offHeapStore;
  }

  @Override
  protected OffHeapDiskStore<String, byte[]> createAndInitStore(TimeSource timeSource, Expiry<? super String, ? super byte[]> expiry, EvictionVeto<? super String, ? super byte[]> evictionVeto) {
    SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(null);
    ClassLoader classLoader = getClass().getClassLoader();
    Serializer<String> serializer = serializationProvider.createValueSerializer(String.class, classLoader);
    Serializer<byte[]> byteArraySerializer = serializationProvider.createValueSerializer(byte[].class, classLoader);
    StoreConfigurationImpl<String, byte[]> storeConfiguration = new StoreConfigurationImpl<String, byte[]>(String.class, byte[].class, evictionVeto, null, getClass().getClassLoader(), expiry, null);
    OffHeapDiskStore<String, byte[]> offHeapStore = new OffHeapDiskStore<String, byte[]>(getPersistenceContext(), storeConfiguration, serializer, byteArraySerializer, timeSource, MemoryUnit.MB.toBytes(1));
    OffHeapDiskStore.Provider.init(offHeapStore);
    return offHeapStore;
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
    serviceLocator.startAllServices();
    provider.start(serviceLocator);
    Store.PersistentStoreConfiguration<String, String, String> storeConfig = mock(Store.PersistentStoreConfiguration.class);
    when(storeConfig.getKeyType()).thenReturn(String.class);
    when(storeConfig.getValueType()).thenReturn(String.class);
    when(storeConfig.getResourcePools()).thenReturn(ResourcePoolsBuilder.newResourcePoolsBuilder()
        .disk(10, MemoryUnit.MB)
        .build());
    try {
      provider.createStore(storeConfig);
      fail("IllegalStateException expected");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("No LocalPersistenceService could be found - did you configure it at the CacheManager level?"));
    }

  }

  private FileBasedPersistenceContext getPersistenceContext() {
    try {
      return persistenceService.createPersistenceContext("cache", mock(Store.PersistentStoreConfiguration.class));
    } catch (CachePersistenceException e) {
      throw new AssertionError(e);
    }
  }

}