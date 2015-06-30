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
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.TimeSource;
import org.ehcache.spi.cache.AbstractValueHolder;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.SerializationProvider;
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
    TestTimeSource timeSource = new TestTimeSource();
    OffHeapStore<String, String> offHeapStore = createAndInitStore(timeSource, Expirations.timeToIdleExpiration(new Duration(15L, TimeUnit.MILLISECONDS)));

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
    OffHeapStore<String, byte[]> offHeapStore = new OffHeapStore<String, byte[]>(storeConfiguration, serializer, byteArraySerializer, timeSource, MemoryUnit.MB.toBytes(1));
    OffHeapStore.Provider.init(offHeapStore);
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

  @Test
  public void testFlushUpdatesAccessStats() throws CacheAccessException {
    final TestTimeSource timeSource = new TestTimeSource();
    final Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(new Duration(15L, TimeUnit.MILLISECONDS));
    final OffHeapStore<String, String> store = createAndInitStore(timeSource, expiry);
    final String key = "foo";
    final String value = "bar";
    store.put(key, value);
    final Store.ValueHolder<String> valueHolder = store.getAndFault(key);
    timeSource.advanceTime(10);
    ((AbstractValueHolder)valueHolder).accessed(timeSource.getTimeMillis(), expiry.getExpiryForAccess(key, value));
    store.flush(key, new DelegatingValueHolder<String>(valueHolder));
    timeSource.advanceTime(10); // this should NOT affect
    assertThat(store.getAndFault(key).lastAccessTime(TimeUnit.MILLISECONDS), is(valueHolder.creationTime(TimeUnit.MILLISECONDS) + 10));
  }

  private OffHeapStore<String, String> createAndInitStore(final TestTimeSource timeSource, final Expiry<Object, Object> expiry) {
    SerializationProvider serializationProvider = new DefaultSerializationProvider();
    serializationProvider.start(null, null);
    ClassLoader classLoader = getClass().getClassLoader();
    Serializer<String> serializer = serializationProvider.createValueSerializer(String.class, classLoader);
    StoreConfigurationImpl<String, String> storeConfiguration = new StoreConfigurationImpl<String, String>(String.class, String.class, null, null, classLoader, expiry, null);
    OffHeapStore<String, String> offHeapStore = new OffHeapStore<String, String>(storeConfiguration, serializer, serializer, timeSource, MemoryUnit.MB.toBytes(1));
    OffHeapStore.Provider.init(offHeapStore);
    return offHeapStore;
  }

  private static class TestStoreEventListener<K, V> implements StoreEventListener<K, V> {

    @Override
    public void onEviction(final K key, final Store.ValueHolder<V> valueHolder) {
      System.out.println("Evicted " + key);
    }

    @Override
    public void onExpiration(final K key, final Store.ValueHolder<V> valueHolder) {
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

  private static class DelegatingValueHolder<T> implements Store.ValueHolder<T> {

    private final Store.ValueHolder<T> valueHolder;

    public DelegatingValueHolder(final Store.ValueHolder<T> valueHolder) {
      this.valueHolder = valueHolder;
    }

    @Override
    public T value() {
      return valueHolder.value();
    }

    @Override
    public long creationTime(final TimeUnit unit) {
      return valueHolder.creationTime(unit);
    }

    @Override
    public long expirationTime(final TimeUnit unit) {
      return valueHolder.expirationTime(unit);
    }

    @Override
    public boolean isExpired(final long expirationTime, final TimeUnit unit) {
      return valueHolder.isExpired(expirationTime, unit);
    }

    @Override
    public long lastAccessTime(final TimeUnit unit) {
      return valueHolder.lastAccessTime(unit);
    }

    @Override
    public float hitRate(final TimeUnit unit) {
      return valueHolder.hitRate(unit);
    }

    @Override
    public long getId() {
      return valueHolder.getId();
    }
  }
}