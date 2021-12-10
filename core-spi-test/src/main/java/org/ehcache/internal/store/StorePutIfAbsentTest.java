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

import org.ehcache.ValueSupplier;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;


/**
 * Test the {@link Store#putIfAbsent(Object, Object)} contract of the
 * {@link Store Store} interface.
 *
 * @author Aurelien Broszniowski
 */

public class StorePutIfAbsentTest<K, V> extends SPIStoreTester<K, V> {

  public StorePutIfAbsentTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  protected Store<K, V> kvStore;
  protected Store kvStore2;

  @After
  public void tearDown() {
    if (kvStore != null) {
      factory.close(kvStore);
      kvStore = null;
    }
    if (kvStore2 != null) {
      @SuppressWarnings("unchecked")
      Store<K, V> kvStore2 = this.kvStore2;
      factory.close(kvStore2);
      this.kvStore2 = null;
    }
  }

  @SPITest
  public void mapsKeyToValueWhenMappingDoesntExist()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = factory.createKey(1);
    V value = factory.createValue(1);

    try {
      assertThat(kvStore.putIfAbsent(key, value), is(nullValue()));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void doesntMapKeyToValueWhenMappingExists()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = factory.createKey(1);
    V value = factory.createValue(1);

    try {
      kvStore.put(key, value);
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }

    V updatedValue = factory.createValue(2);

    try {
      assertThat(kvStore.putIfAbsent(key, updatedValue).value(), is(equalTo(value)));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void nullKeyThrowsException()
      throws StoreAccessException, IllegalAccessException, InstantiationException {
    kvStore = factory.newStore();

    K key = null;
    V value = factory.createValue(1);

    try {
      kvStore.putIfAbsent(key, value);
      throw new AssertionError("Expected NullPointerException because the key is null");
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SPITest
  public void nullValueThrowsException()
      throws StoreAccessException, IllegalAccessException, InstantiationException {
    kvStore = factory.newStore();

    K key = factory.createKey(1);
    V value = null;

    try {
      kvStore.putIfAbsent(key, value);
      throw new AssertionError("Expected NullPointerException because the value is null");
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SPITest
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void wrongKeyTypeThrowsException()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore2 = factory.newStore();

    V value = factory.createValue(1);

    try {
      if (this.factory.getKeyType() == String.class) {
        kvStore2.putIfAbsent(1.0f, value);
      } else {
        kvStore2.putIfAbsent("key", value);
      }
      throw new AssertionError("Expected ClassCastException because the key is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void wrongValueTypeThrowsException()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore2 = factory.newStore();

    K key = factory.createKey(1);

    try {
      if (this.factory.getValueType() == String.class) {
        kvStore2.putIfAbsent(key, 1.0f);
      } else {
        kvStore2.putIfAbsent(key, "value");
      }
      throw new AssertionError("Expected ClassCastException because the value is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testPutIfAbsentValuePresentExpiresOnAccess() throws LegalSPITesterException {
    TestTimeSource timeSource = new TestTimeSource(10043L);
    kvStore = factory.newStoreWithExpiry(Expirations.builder().setAccess(Duration.ZERO).build(), timeSource);

    K key = factory.createKey(250928L);
    V value = factory.createValue(2059820L);
    V newValue = factory.createValue(205982025L);

    try {
      kvStore.put(key, value);
      assertThat(kvStore.putIfAbsent(key, newValue).value(), is(value));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }
}
