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
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

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
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testPutIfAbsentCreates() throws LegalSPITesterException {
    StoreEventListener<K, V> listener = addListener(store);

    try {
      store.putIfAbsent(factory.createKey(42L), factory.createValue(42L), b -> {});
      verifyListenerInteractions(listener);
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testComputeCreates() throws LegalSPITesterException {
    StoreEventListener<K, V> listener = addListener(store);

    try {
      store.getAndCompute(factory.createKey(125L), (k, v) -> factory.createValue(215L));
      verifyListenerInteractions(listener);
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testComputeIfAbsentCreates() throws LegalSPITesterException {
    StoreEventListener<K, V> listener = addListener(store);

    try {
      store.computeIfAbsent(factory.createKey(125L), k -> factory.createValue(125L));
      verifyListenerInteractions(listener);
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  private void verifyListenerInteractions(StoreEventListener<K, V> listener) {
    Matcher<StoreEvent<K, V>> matcher = eventType(EventType.CREATED);
    verify(listener).onEvent(argThat(matcher));
  }

  private StoreEventListener<K, V> addListener(Store<K, V> kvStore) {
    @SuppressWarnings("unchecked")
    StoreEventListener<K, V> listener = mock(StoreEventListener.class);

    kvStore.getStoreEventSource().addEventListener(listener);
    return listener;
  }

  public static <K, V> Matcher<StoreEvent<K, V>> eventType(final EventType type) {
    return new TypeSafeMatcher<StoreEvent<K, V>>() {
      @Override
      protected boolean matchesSafely(StoreEvent<K, V> item) {
        return item.getType().equals(type);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("store event of type '").appendValue(type).appendText("'");
      }
    };
  }
}
