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
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.persistence.DefaultLocalPersistenceService;
import org.ehcache.internal.store.offheap.AbstractOffHeapStoreTest;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.FileBasedPersistenceContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.ehcache.expiry.Expirations.noExpiration;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class OffHeapDiskStoreTest extends AbstractOffHeapStoreTest {

  private static DefaultLocalPersistenceService persistenceService;
  private static FileBasedPersistenceContext persistenceContext;
  
  @BeforeClass
  public static void classSetUp() throws Exception {
    persistenceService = new DefaultLocalPersistenceService(new PersistenceConfiguration(
            new File(OffHeapDiskStoreTest.class.getClassLoader().getResource(".").toURI().getPath(), "disk-store-spi-test")));
    persistenceService.start(null, null);
    Store.PersistentStoreConfiguration persistentStoreConfiguration = mock(Store.PersistentStoreConfiguration.class);
    persistenceContext = persistenceService.createPersistenceContext("cache", persistentStoreConfiguration);
  }

  @AfterClass
  public static void classTearDown() {
    try {
      persistenceService.destroyPersistenceContext("cache");
    } finally {
      persistenceService.stop();
    }
  }
  
  @Ignore
  public void testEvictionVeto() {}
  
  @Test
  public void testRecovery() throws CacheAccessException, IOException {
    OffHeapDiskStore<String, String> offHeapDiskStore = createAndInitStore(SystemTimeSource.INSTANCE, noExpiration());

    offHeapDiskStore.put("key1", "value1");
    assertThat(offHeapDiskStore.get("key1"), notNullValue());
    
    OffHeapDiskStore.Provider.close(offHeapDiskStore);
    
    OffHeapDiskStore.Provider.init(offHeapDiskStore);
    assertThat(offHeapDiskStore.get("key1"), notNullValue());
  }
  
  @Override
  protected OffHeapDiskStore<String, String> createAndInitStore(final TimeSource timeSource, final Expiry<? super String, ? super String> expiry) {
    SerializationProvider serializationProvider = new DefaultSerializationProvider();
    serializationProvider.start(null, null);
    ClassLoader classLoader = getClass().getClassLoader();
    Serializer<String> serializer = serializationProvider.createValueSerializer(String.class, classLoader);
    StoreConfigurationImpl<String, String> storeConfiguration = new StoreConfigurationImpl<String, String>(String.class, String.class, null, null, classLoader, expiry, null);
    OffHeapDiskStore<String, String> offHeapStore = new OffHeapDiskStore<String, String>(persistenceContext, storeConfiguration, serializer, serializer, timeSource, MemoryUnit.MB.toBytes(1));
    OffHeapDiskStore.Provider.init(offHeapStore);
    return offHeapStore;
  }

  @Override
  protected OffHeapDiskStore<String, byte[]> createAndInitStore(TimeSource timeSource, Expiry<? super String, ? super byte[]> expiry, EvictionVeto<? super String, ? super byte[]> evictionVeto) {
    SerializationProvider serializationProvider = new DefaultSerializationProvider();
    serializationProvider.start(null, null);
    ClassLoader classLoader = getClass().getClassLoader();
    Serializer<String> serializer = serializationProvider.createValueSerializer(String.class, classLoader);
    Serializer<byte[]> byteArraySerializer = serializationProvider.createValueSerializer(byte[].class, classLoader);
    StoreConfigurationImpl<String, byte[]> storeConfiguration = new StoreConfigurationImpl<String, byte[]>(String.class, byte[].class, evictionVeto, null, getClass().getClassLoader(), expiry, null);
    OffHeapDiskStore<String, byte[]> offHeapStore = new OffHeapDiskStore<String, byte[]>(persistenceContext, storeConfiguration, serializer, byteArraySerializer, timeSource, MemoryUnit.MB.toBytes(1));
    OffHeapDiskStore.Provider.init(offHeapStore);
    return offHeapStore;
  }

}