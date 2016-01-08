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
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;
import org.mockito.InOrder;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * StoreCreationEventListenerTest
 */
public class StoreCreationEventListenerTest<K, V> extends SPIStoreTester<K, V> {


  private Store<K, V> store;

  public StoreCreationEventListenerTest(StoreFactory<K, V> factory) {
    super(factory);
  }

  @Before
  public void setUp() {
    store = factory.newStore();
  }

  @After
  public void tearDown() {
    if (store != null) {
      factory.close(store);
      store = null;
    }
  }

  @SPITest
  public void testPutCreates() throws LegalSPITesterException {
    StoreEventListener<K, V> listener = addListener(store);

    try {
      store.put(factory.createKey(1L), factory.createValue(1L));
      verifyListenerInteractions(listener);
    } catch (CacheAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testPutIfAbsentCreates() throws LegalSPITesterException {
    StoreEventListener<K, V> listener = addListener(store);

    try {
      store.putIfAbsent(factory.createKey(42L), factory.createValue(42L));
      verifyListenerInteractions(listener);
    } catch (CacheAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testComputeCreates() throws LegalSPITesterException {
    StoreEventListener<K, V> listener = addListener(store);

    try {
      store.compute(factory.createKey(125L), new BiFunction<K, V, V>() {
        @Override
        public V apply(K k, V v) {
          return factory.createValue(215L);
        }
      });
      verifyListenerInteractions(listener);
    } catch (CacheAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testComputeIfAbsentCreates() throws LegalSPITesterException {
    StoreEventListener<K, V> listener = addListener(store);

    try {
      store.computeIfAbsent(factory.createKey(125L), new Function<K, V>() {
        @Override
        public V apply(K k) {
          return factory.createValue(125L);
        }
      });
      verifyListenerInteractions(listener);
    } catch (CacheAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  private void verifyListenerInteractions(StoreEventListener<K, V> listener) {InOrder inOrder = inOrder(listener);
    inOrder.verify(listener).hasListeners();
    inOrder.verify(listener).onCreation(any(factory.getKeyType()), any(Store.ValueHolder.class));
    inOrder.verify(listener).fireAllEvents();
    inOrder.verify(listener).purgeOrFireRemainingEvents();
    inOrder.verifyNoMoreInteractions();
  }

  private StoreEventListener<K, V> addListener(Store<K, V> kvStore) {
    StoreEventListener<K, V> listener = mock(StoreEventListener.class);
    when(listener.hasListeners()).thenReturn(true);

    kvStore.enableStoreEventNotifications(listener);
    return listener;
  }
}
