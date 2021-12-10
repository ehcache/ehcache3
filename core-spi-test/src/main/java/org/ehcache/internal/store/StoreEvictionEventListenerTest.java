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

import org.ehcache.Cache;
import org.ehcache.core.spi.store.Store;
import org.ehcache.event.EventType;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Ignore;
import org.ehcache.spi.test.SPITest;
import org.hamcrest.Matcher;

import static org.ehcache.internal.store.StoreCreationEventListenerTest.eventType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * Tests eviction events according to the contract of the
 * {@link Store Store} interface.
 */
public class StoreEvictionEventListenerTest<K, V> extends SPIStoreTester<K, V> {

  public StoreEvictionEventListenerTest(StoreFactory<K, V> factory) {
    super(factory);
  }

  final K k = factory.createKey(1L);
  final V v = factory.createValue(1L);
  final K k2 = factory.createKey(2L);
  final V v2 = factory.createValue(2L);
  final V v3 = factory.createValue(3L);

  protected Store<K, V> kvStore;

  @After
  public void tearDown() {
    if (kvStore != null) {
      factory.close(kvStore);
      kvStore = null;
    }
  }

  @SPITest
  public void testPutOnEviction() throws Exception {
    kvStore = factory.newStoreWithCapacity(1L);
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    kvStore.put(k2, v2);
    verifyListenerInteractions(listener);
  }

  @SPITest
  public void testPutIfAbsentOnEviction() throws Exception {
    kvStore = factory.newStoreWithCapacity(1L);
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    kvStore.putIfAbsent(k2, v2, b -> {});
    verifyListenerInteractions(listener);
  }

  @SPITest
  @Ignore(reason = "See comment")
  public void testReplaceTwoArgsOnEviction() throws Exception {
    // The following makes no sense, what we may want to test here is that replace with a bigger value evicts
    // But that would also mean supporting the fact that this may not impact the store (think count based)
    kvStore = factory.newStoreWithCapacity(1L);
    StoreEventListener<K, V> listener = addListener(kvStore);
    kvStore.put(k, v);
    kvStore.put(k2, v2);
    verifyListenerInteractions(listener);
    kvStore.replace(getOnlyKey(kvStore.iterator()), v3);
    assertThat(kvStore.get(getOnlyKey(kvStore.iterator())).get(), is(v3));
  }

  @SPITest
  public void testComputeOnEviction() throws Exception {
    kvStore = factory.newStoreWithCapacity(1L);
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    kvStore.getAndCompute(k2, (mappedKey, mappedValue) -> v2);
    verifyListenerInteractions(listener);
  }

  @SPITest
  public void testComputeIfAbsentOnEviction() throws Exception {
    kvStore = factory.newStoreWithCapacity(1L);
    kvStore.put(k, v);
    StoreEventListener<K, V> listener = addListener(kvStore);
    kvStore.computeIfAbsent(k2, mappedKey -> v2);
    verifyListenerInteractions(listener);
  }

  private K getOnlyKey(Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iter)
      throws StoreAccessException {
    if (iter.hasNext()) {
      Cache.Entry<K, Store.ValueHolder<V>> entry = iter.next();
      return entry.getKey();
    }
    return null;
  }

  private void verifyListenerInteractions(StoreEventListener<K, V> listener) {
    Matcher<StoreEvent<K, V>> matcher = eventType(EventType.EVICTED);
    verify(listener).onEvent(argThat(matcher));
  }

  private StoreEventListener<K, V> addListener(Store<K, V> kvStore) {
    @SuppressWarnings("unchecked")
    StoreEventListener<K, V> listener = mock(StoreEventListener.class);

    kvStore.getStoreEventSource().addEventListener(listener);
    return listener;
  }
}

