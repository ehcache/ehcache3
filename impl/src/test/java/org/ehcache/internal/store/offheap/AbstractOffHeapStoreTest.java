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

import java.util.concurrent.TimeUnit;
import org.ehcache.Cache;
import org.ehcache.config.EvictionVeto;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.TimeSource;
import org.ehcache.spi.cache.AbstractValueHolder;
import org.ehcache.spi.cache.Store;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author cdennis
 */
public abstract class AbstractOffHeapStoreTest {
  
  @Test
  public void testWriteBackOfValueHolder() throws CacheAccessException {
    TestTimeSource timeSource = new TestTimeSource();
    AbstractOffHeapStore<String, String> offHeapStore = createAndInitStore(timeSource, Expirations.timeToIdleExpiration(new Duration(15L, TimeUnit.MILLISECONDS)));
    try {
      offHeapStore.put("key1", "value1");
      timeSource.advanceTime(10);
      OffHeapValueHolder<String> valueHolder = (OffHeapValueHolder<String>)offHeapStore.get("key1");
      assertThat(valueHolder.lastAccessTime(TimeUnit.MILLISECONDS), is(10L));
      timeSource.advanceTime(10);
      assertThat(offHeapStore.get("key1"), notNullValue());
      timeSource.advanceTime(16);
      assertThat(offHeapStore.get("key1"), nullValue());
    } finally {
      destroyStore(offHeapStore);
    }
  }

  @Test
  public void testEvictionVeto() throws CacheAccessException {
    TestTimeSource timeSource = new TestTimeSource();
    Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(new Duration(15L, TimeUnit.MILLISECONDS));
    EvictionVeto<String, byte[]> evictionVeto = new EvictionVeto<String, byte[]>() {

      @Override
      public boolean test(Cache.Entry<String, byte[]> entry) {
//        return entry.getKey().equals("key3");
        return true;
      }
    };
    AbstractOffHeapStore<String, byte[]> offHeapStore = createAndInitStore(timeSource, expiry, evictionVeto);
    try {
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
      //offHeapStore.put("key7", value);
    } finally {
      destroyStore(offHeapStore);
    }

  }

  @Test
  public void testFlushUpdatesAccessStats() throws CacheAccessException {
    final TestTimeSource timeSource = new TestTimeSource();
    final Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(new Duration(15L, TimeUnit.MILLISECONDS));
    final AbstractOffHeapStore<String, String> store = createAndInitStore(timeSource, expiry);
    try {
      final String key = "foo";
      final String value = "bar";
      store.put(key, value);
      final Store.ValueHolder<String> firstValueHolder = store.getAndFault(key);
      store.put(key, value);
      final Store.ValueHolder<String> secondValueHolder = store.getAndFault(key);
      timeSource.advanceTime(10);
      ((AbstractValueHolder)firstValueHolder).accessed(timeSource.getTimeMillis(), expiry.getExpiryForAccess(key, value));
      timeSource.advanceTime(10);
      ((AbstractValueHolder)secondValueHolder).accessed(timeSource.getTimeMillis(), expiry.getExpiryForAccess(key, value));
      assertThat(store.flush(key, new DelegatingValueHolder<String>(firstValueHolder)), is(false));
      assertThat(store.flush(key, new DelegatingValueHolder<String>(secondValueHolder)), is(true));
      timeSource.advanceTime(10); // this should NOT affect
      assertThat(store.getAndFault(key).lastAccessTime(TimeUnit.MILLISECONDS), is(secondValueHolder.creationTime(TimeUnit.MILLISECONDS) + 20));
    } finally {
      destroyStore(store);
    }
  }

  protected abstract AbstractOffHeapStore<String, String> createAndInitStore(final TimeSource timeSource, final Expiry<? super String, ? super String> expiry);

  protected abstract AbstractOffHeapStore<String, byte[]> createAndInitStore(final TimeSource timeSource, final Expiry<? super String, ? super byte[]> expiry, EvictionVeto<? super String, ? super byte[]> evictionVeto);

  protected abstract void destroyStore(AbstractOffHeapStore<?, ?> store);
  
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

  public static class DelegatingValueHolder<T> implements Store.ValueHolder<T> {

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
