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

package org.ehcache.internal.store;

import org.ehcache.events.StoreEventListener;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.SPITest;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;

/**
 * Test the expiry half of the {@link org.ehcache.spi.cache.Store#enableStoreEventNotifications(org.ehcache.events.StoreEventListener)} contract of the
 * {@link org.ehcache.spi.cache.Store Store} interface.
 * <p/>
 *
 * @author Gaurav Mangalick
 */

public class StoreExpiryEventListenerTest<K, V> extends SPIStoreTester<K, V> {

  public StoreExpiryEventListenerTest(StoreFactory<K, V> factory) {
    super(factory);
  }

  final K k = factory.createKey(1L);
  final V v = factory.createValue(1l);
  final K k2 = factory.createKey(2L);
  final V v2 = factory.createValue(2l);
  final V v3 = factory.createValue(3l);

  protected Store<K, V> kvStore;

  @After
  public void tearDown() {
    if (kvStore != null) {
//      kvStore.close();
      kvStore = null;
    }
  }

  @SPITest
  public void testGetOnExpiration() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    kvStore = factory.newStoreWithExpiry(Expirations.<K, V>timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)), timeSource);
    TestStoreEventListener listener = new TestStoreEventListener();
    kvStore.enableStoreEventNotifications(listener);
    kvStore.put(k, v);
    timeSource.advanceTime(1);
    assertThat(kvStore.get(k), is(nullValue()));
    assertThat(listener.expired, hasItem(k));
  }

  @SPITest
  public void testContainsKeyOnExpiration() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    kvStore = factory.newStoreWithExpiry(Expirations.<K, V>timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)), timeSource);
    TestStoreEventListener listener = new TestStoreEventListener();
    kvStore.enableStoreEventNotifications(listener);
    kvStore.put(k, v);
    timeSource.advanceTime(1);
    assertThat(kvStore.containsKey(k), is(false));
    assertThat(listener.expired, hasItem(k));
  }

  @SPITest
  public void testPutIfAbsentOnExpiration() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    kvStore = factory.newStoreWithExpiry(Expirations.<K, V>timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)), timeSource);
    TestStoreEventListener listener = new TestStoreEventListener();
    kvStore.enableStoreEventNotifications(listener);
    kvStore.put(k, v);
    timeSource.advanceTime(1);
    Store.ValueHolder<V> lastValue = kvStore.putIfAbsent(k, v);
    assertThat(lastValue, is(nullValue()));
    assertThat(listener.expired, hasItem(k));
  }

  @SPITest
  public void testRemoveOnExpiration() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    kvStore = factory.newStoreWithExpiry(Expirations.<K, V>timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)), timeSource);
    TestStoreEventListener listener = new TestStoreEventListener();
    kvStore.enableStoreEventNotifications(listener);
    kvStore.put(k, v);
    timeSource.advanceTime(1);
    assertThat(kvStore.remove(k, v), is(false));
    assertThat(listener.expired, hasItem(k));
  }

  @SPITest
  public void testReplaceTwoArgsOnExpiration() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    kvStore = factory.newStoreWithExpiry(Expirations.<K, V>timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)), timeSource);
    TestStoreEventListener listener = new TestStoreEventListener();
    kvStore.enableStoreEventNotifications(listener);
    kvStore.put(k, v);
    timeSource.advanceTime(1);
    assertThat(kvStore.replace(k, v), is(nullValue()));
    assertThat(listener.expired, hasItem(k));
  }

  @SPITest
  public void testReplaceThreeArgsOnExpiration() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    kvStore = factory.newStoreWithExpiry(Expirations.<K, V>timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)), timeSource);
    TestStoreEventListener listener = new TestStoreEventListener();
    kvStore.enableStoreEventNotifications(listener);
    kvStore.put(k, v);
    timeSource.advanceTime(1);
    assertThat(kvStore.replace(k, v, v2), is(false));
    assertThat(listener.expired, hasItem(k));
  }

  @SPITest
  public void testComputeOnExpiration() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    kvStore = factory.newStoreWithExpiry(Expirations.<K, V>timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)), timeSource);
    TestStoreEventListener listener = new TestStoreEventListener();
    kvStore.enableStoreEventNotifications(listener);
    kvStore.put(k, v);
    timeSource.advanceTime(1);
    kvStore.compute(k, new BiFunction<K, V, V>() {
      @Override
      public V apply(K mappedKey, V mappedValue) {
        return v2;
      }
    });
    assertThat(kvStore.get(k).value(), is(v2));
    assertThat(listener.expired, hasItem(k));
  }

  @SPITest
  public void testComputeIfAbsentOnExpiration() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    kvStore = factory.newStoreWithExpiry(Expirations.<K, V>timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)), timeSource);
    TestStoreEventListener listener = new TestStoreEventListener();
    kvStore.enableStoreEventNotifications(listener);
    kvStore.put(k, v);
    timeSource.advanceTime(1);

    kvStore.computeIfAbsent(k, new Function<K, V>() {
      @Override
      public V apply(K mappedKey) {
        return v2;
      }
    });
    assertThat(kvStore.get(k).value(), is(v2));
    assertThat(listener.expired, hasItem(k));
  }

  @SPITest
  public void testComputeIfPresentOnExpiration() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    kvStore = factory.newStoreWithExpiry(Expirations.<K, V>timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)), timeSource);
    TestStoreEventListener listener = new TestStoreEventListener();
    kvStore.enableStoreEventNotifications(listener);
    kvStore.put(k, v);
    timeSource.advanceTime(1);

    kvStore.computeIfPresent(k, new BiFunction<K, V, V>() {
      @Override
      public V apply(K mappedKey, V mappedValue) {
        throw new AssertionError();
      }
    });
    assertThat(kvStore.get(k), is(nullValue()));
    assertThat(listener.expired, hasItem(k));
  }

  private class TestStoreEventListener implements StoreEventListener<K, V> {
    private final Set<K> expired = new HashSet<K>();

    @Override
    public void onEviction(final K key, final Store.ValueHolder<V> valueHolder) {
      fail("Unexpected eviction");
    }

    @Override
    public void onExpiration(final K key, final Store.ValueHolder<V> valueHolder) {
      expired.add(key);
    }
  }


}

