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

import org.ehcache.Cache;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.persistence.DefaultLocalPersistenceService;
import org.ehcache.internal.store.offheap.OffHeapValueHolder;
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
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class OffHeapDiskStoreTest {

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
  
  @Test
  public void testWriteBackOfValueHolder() throws CacheAccessException {
    SerializationProvider serializationProvider = new DefaultSerializationProvider();
    serializationProvider.start(null, null);
    ClassLoader classLoader = getClass().getClassLoader();
    Serializer<String> serializer = serializationProvider.createValueSerializer(String.class, classLoader);
    TestTimeSource timeSource = new TestTimeSource();
    Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(new Duration(15L, TimeUnit.MILLISECONDS));
    StoreConfigurationImpl<String, String> storeConfiguration = new StoreConfigurationImpl<String, String>(String.class, String.class, null, null, classLoader, expiry, null);
    OffHeapDiskStore<String, String> offHeapDiskStore = new OffHeapDiskStore<String, String>(persistenceContext, storeConfiguration, serializer, serializer, timeSource, MemoryUnit.MB.toBytes(1));
    OffHeapDiskStore.Provider.init(offHeapDiskStore);

    offHeapDiskStore.put("key1", "value1");
    timeSource.advanceTime(10);
    OffHeapValueHolder<String> valueHolder = (OffHeapValueHolder<String>)offHeapDiskStore.get("key1");
    assertThat(valueHolder.lastAccessTime(TimeUnit.MILLISECONDS), is(10L));
    timeSource.advanceTime(10);
    assertThat(offHeapDiskStore.get("key1"), notNullValue());
    timeSource.advanceTime(16);
    assertThat(offHeapDiskStore.get("key1"), nullValue());
  }

  @Ignore
  @Test
  public void testEvictionVeto() throws CacheAccessException {
    SerializationProvider serializationProvider = new DefaultSerializationProvider();
    serializationProvider.start(null, null);
    ClassLoader classLoader = getClass().getClassLoader();
    Serializer<String> serializer = serializationProvider.createValueSerializer(String.class, classLoader);
    Serializer<byte[]> byteArraySerializer = serializationProvider.createValueSerializer(byte[].class, classLoader);
    TestTimeSource timeSource = new TestTimeSource();
    Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(new Duration(15L, TimeUnit.MILLISECONDS));
    EvictionVeto<String, byte[]> evictionVeto = new EvictionVeto<String, byte[]>() {

      @Override
      public boolean test(Cache.Entry<String, byte[]> entry) {
//        return entry.getKey().equals("key3");
        return true;
      }
    };
    StoreConfigurationImpl<String, byte[]> storeConfiguration = new StoreConfigurationImpl<String, byte[]>(String.class, byte[].class, evictionVeto, null, classLoader, expiry, null);
    OffHeapDiskStore<String, byte[]> offHeapDiskStore = new OffHeapDiskStore<String, byte[]>(persistenceContext, storeConfiguration, serializer, byteArraySerializer, timeSource, MemoryUnit.MB.toBytes(1));
    OffHeapDiskStore.Provider.init(offHeapDiskStore);
    offHeapDiskStore.enableStoreEventNotifications(new TestStoreEventListener<String, byte[]>());

    int valueLength = 200000;
    byte[] value = new byte[valueLength];
    value[0] = 1;
    value[valueLength/2] = 1;
    value[valueLength - 1] = 1;
    offHeapDiskStore.put("key1", value);
    offHeapDiskStore.put("key2", value);
    offHeapDiskStore.put("key3", value);
    offHeapDiskStore.put("key4", value);
    offHeapDiskStore.put("key5", value);
    offHeapDiskStore.put("key6", value);
//    offHeapStore.put("key7", value);

  }

  private static class TestStoreEventListener<K, V> implements StoreEventListener<K, V> {

    @Override
    public void onEviction(K key, Store.ValueHolder<V> valueHolder) {
      System.out.println("Evicted " + key);
    }

    @Override
    public void onExpiration(K key, Store.ValueHolder<V> valueHolder) {
      System.out.println("Expired " + key);
    }
  }

  private static class TestTimeSource implements TimeSource {

    private long time = 0;

    @Override
    public long getTimeMillis() {
      return time;
    }

    public void advanceTime(long step) {
      time += step;
    }
  }

}