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

package org.ehcache.internal.store.offheap;

import org.ehcache.Cache;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class OffHeapStoreTest {

  @Test
  public void testWriteBackOfValueHolder() throws CacheAccessException {
    JavaSerializationProvider serializationProvider = new JavaSerializationProvider();
    ClassLoader classLoader = getClass().getClassLoader();
    Serializer<String> serializer = serializationProvider.createSerializer(String.class, classLoader);
    TestTimeSource timeSource = new TestTimeSource();
    Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(new Duration(15L, TimeUnit.MILLISECONDS));
    StoreConfigurationImpl<String, String> storeConfiguration = new StoreConfigurationImpl<String, String>(String.class, String.class, 10L, null, null, classLoader, expiry);
    OffHeapStore<String, String> offHeapStore = new OffHeapStore<String, String>(storeConfiguration, serializer, serializer, timeSource, 1024 * 1024);

    offHeapStore.put("key1", "value1");
    timeSource.advanceTime(10);
    OffHeapValueHolder<String> valueHolder = (OffHeapValueHolder<String>)offHeapStore.get("key1");
    assertThat(valueHolder.lastAccessTime(TimeUnit.MILLISECONDS), is(10L));
    timeSource.advanceTime(10);
    assertThat(offHeapStore.get("key1"), notNullValue());
    timeSource.advanceTime(16);
    assertThat(offHeapStore.get("key1"), nullValue());
  }

  @Test
  public void testEvictionVeto() throws CacheAccessException {
    JavaSerializationProvider serializationProvider = new JavaSerializationProvider();
    ClassLoader classLoader = getClass().getClassLoader();
    Serializer<String> serializer = serializationProvider.createSerializer(String.class, classLoader);
    Serializer<byte[]> byteArraySerializer = serializationProvider.createSerializer(byte[].class, classLoader);
    TestTimeSource timeSource = new TestTimeSource();
    Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(new Duration(15L, TimeUnit.MILLISECONDS));
    EvictionVeto<String, byte[]> evictionVeto = new EvictionVeto<String, byte[]>() {

      @Override
      public boolean test(Cache.Entry<String, byte[]> entry) {
//        return entry.getKey().equals("key3");
        return true;
      }
    };
    StoreConfigurationImpl<String, byte[]> storeConfiguration = new StoreConfigurationImpl<String, byte[]>(String.class, byte[].class, 10L, evictionVeto, null, classLoader, expiry);
    OffHeapStore<String, byte[]> offHeapStore = new OffHeapStore<String, byte[]>(storeConfiguration, serializer, byteArraySerializer, timeSource, 1024 * 1024);
    offHeapStore.enableStoreEventNotifications(new TestStoreEventListener<String, byte[]>());

    int valueLength = 200000;
    byte[] value = new byte[valueLength];
    value[0] = 1;
    value[valueLength/2] = 1;
    value[valueLength - 1] = 1;
    offHeapStore.put("key1", value);
    offHeapStore.put("key2", value);
    offHeapStore.put("key3", value);
    offHeapStore.put("key4", value);
    offHeapStore.put("key5", value);
    offHeapStore.put("key6", value);
//    offHeapStore.put("key7", value);

  }

  private static class TestStoreEventListener<K, V> implements StoreEventListener<K, V> {

    @Override
    public void onEviction(Cache.Entry<K, V> entry) {
      System.out.println("Evicted " + entry.getKey());
    }

    @Override
    public void onExpiration(Cache.Entry<K, V> entry) {
      System.out.println("Expired " + entry.getKey());
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