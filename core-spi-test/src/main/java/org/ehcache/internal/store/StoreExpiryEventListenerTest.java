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

import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.RemoveStatus;
import org.ehcache.core.spi.store.Store.ReplaceStatus;
import org.ehcache.event.EventType;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.SPITest;
import org.hamcrest.Matcher;

import java.util.concurrent.TimeUnit;

import static org.ehcache.internal.store.StoreCreationEventListenerTest.eventType;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests expiry events according to the contract of the
 * {@link Store Store} interface.
 */
public class StoreExpiryEventListenerTest<K, V> extends SPIStoreTester<K, V> {

  private TestTimeSource timeSource;

  public StoreExpiryEventListenerTest(StoreFactory<K, V> factory) {
    super(factory);
  }

  final K k = factory.createKey(1L);
  final V v = factory.createValue(1l);
  final V v2 = factory.createValue(2l);

  protected Store<K, V> kvStore;

  @Before
  public void setUp() {
    timeSource = new TestTimeSource();
    kvStore = factory.newStoreWithExpiry(Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)), timeSource);
  }

  @SPITest
  public void testGetOnExpiration() throws Exception {
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    timeSource.advanceTime(1);
    assertThat(kvStore.get(k), is(nullValue()));
    verifyListenerInteractions(listener);
  }

  @SPITest
  public void testContainsKeyOnExpiration() throws Exception {
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    timeSource.advanceTime(1);
    assertThat(kvStore.containsKey(k), is(false));
    verifyListenerInteractions(listener);
  }

  @SPITest
  public void testPutIfAbsentOnExpiration() throws Exception {
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    timeSource.advanceTime(1);
    assertThat(kvStore.putIfAbsent(k, v), is(nullValue()));
    verifyListenerInteractions(listener);
  }

  @SPITest
  public void testRemoveOnExpiration() throws Exception {
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    timeSource.advanceTime(1);
    assertThat(kvStore.remove(k), is(false));
    verifyListenerInteractions(listener);
  }

  @SPITest
  public void testConditionalRemoveOnExpiration() throws Exception {
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    timeSource.advanceTime(1);
    assertThat(kvStore.remove(k, v), is(RemoveStatus.KEY_MISSING));
    verifyListenerInteractions(listener);
  }

  @SPITest
  public void testReplaceTwoArgsOnExpiration() throws Exception {
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    timeSource.advanceTime(1);
    assertThat(kvStore.replace(k, v), is(nullValue()));
    verifyListenerInteractions(listener);
  }

  @SPITest
  public void testReplaceThreeArgsOnExpiration() throws Exception {
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    timeSource.advanceTime(1);
    assertThat(kvStore.replace(k, v, v2), is(ReplaceStatus.MISS_NOT_PRESENT));
    verifyListenerInteractions(listener);
  }

  @SPITest
  public void testComputeOnExpiration() throws Exception {
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    timeSource.advanceTime(1);
    assertThat(kvStore.compute(k, new BiFunction<K, V, V>() {
      @Override
      public V apply(K mappedKey, V mappedValue) {
        return v2;
      }
    }).value(), is(v2));
    verifyListenerInteractions(listener);
  }

  @SPITest
  public void testComputeIfAbsentOnExpiration() throws Exception {
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    timeSource.advanceTime(1);

    assertThat(kvStore.computeIfAbsent(k, new Function<K, V>() {
      @Override
      public V apply(K mappedKey) {
        return v2;
      }
    }).value(), is(v2));
    verifyListenerInteractions(listener);
  }

  private void verifyListenerInteractions(StoreEventListener<K, V> listener) {
    Matcher<StoreEvent<K, V>> matcher = eventType(EventType.EXPIRED);
    verify(listener).onEvent(argThat(matcher));
  }

  private StoreEventListener<K, V> addListener(Store<K, V> kvStore) {
    @SuppressWarnings("unchecked")
    StoreEventListener<K, V> listener = mock(StoreEventListener.class);

    kvStore.getStoreEventSource().addEventListener(listener);
    return listener;
  }
}

