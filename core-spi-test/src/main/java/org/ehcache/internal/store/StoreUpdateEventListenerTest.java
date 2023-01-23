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

import org.ehcache.event.EventType;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;
import org.hamcrest.Matcher;

import static org.ehcache.internal.store.StoreCreationEventListenerTest.eventType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * StoreCreationEventListenerTest
 */
public class StoreUpdateEventListenerTest<K, V> extends SPIStoreTester<K, V> {


  private Store<K, V> store;

  public StoreUpdateEventListenerTest(StoreFactory<K, V> factory) {
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
  public void testPutUpdates() throws LegalSPITesterException {

    try {
      K key = factory.createKey(1L);
      store.put(key, factory.createValue(1L));
      StoreEventListener<K, V> listener = addListener(store);
      store.put(key, factory.createValue(123L));
      verifyListenerInteractions(listener);
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testReplace2ArgsUpdates() throws LegalSPITesterException {

    try {
      K key = factory.createKey(1L);
      store.put(key, factory.createValue(1L));
      StoreEventListener<K, V> listener = addListener(store);
      store.replace(key, factory.createValue(123L));
      verifyListenerInteractions(listener);
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testReplace3ArgsUpdates() throws LegalSPITesterException {

    try {
      K key = factory.createKey(1L);
      V value = factory.createValue(1L);
      store.put(key, value);
      StoreEventListener<K, V> listener = addListener(store);
      store.replace(key, value, factory.createValue(123L));
      verifyListenerInteractions(listener);
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testComputeUpdates() throws LegalSPITesterException {

    try {
      K key = factory.createKey(125L);
      store.put(key, factory.createValue(125L));
      StoreEventListener<K, V> listener = addListener(store);
      store.getAndCompute(key, (k, v) -> factory.createValue(215L));
      verifyListenerInteractions(listener);
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  private void verifyListenerInteractions(StoreEventListener<K, V> listener) {
    Matcher<StoreEvent<K, V>> matcher = eventType(EventType.UPDATED);
    verify(listener).onEvent(argThat(matcher));
  }

  private StoreEventListener<K, V> addListener(Store<K, V> kvStore) {
    @SuppressWarnings("unchecked")
    StoreEventListener<K, V> listener = mock(StoreEventListener.class);

    kvStore.getStoreEventSource().addEventListener(listener);
    return listener;
  }
}
