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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;


/**
 * Test the {@link Store#put(Object, Object)} contract of the
 * {@link Store Store} interface.
 *
 * @author Aurelien Broszniowski
 */

public class StorePutTest<K, V> extends SPIStoreTester<K, V> {

  public StorePutTest(final StoreFactory<K, V> factory) {
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
  public void nullKeyThrowsException()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = null;
    V value = factory.createValue(1);

    try {
      kvStore.put(key, value);
      throw new AssertionError("Expected NullPointerException because the key is null");
    } catch (NullPointerException e) {
      // expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void nullValueThrowsException()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = factory.createKey(1);
    V value = null;

    try {
      kvStore.put(key, value);
      throw new AssertionError("Expected NullPointerException because the value is null");
    } catch (NullPointerException e) {
      // expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void indicatesValuePutAndCanBeRetrievedWithEqualKey()
      throws IllegalAccessException, InstantiationException, StoreAccessException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = factory.createKey(1);
    V value = factory.createValue(1);

    try {
      Store.PutStatus putStatus = kvStore.put(key, value);
      assertThat(putStatus, is(Store.PutStatus.PUT));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }

    assertThat(kvStore.get(key), notNullValue());
  }

  @SPITest
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void wrongKeyTypeThrowsException()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore2 = factory.newStore();

    V value = factory.createValue(1);

    try {
      if (this.factory.getKeyType() == String.class) {
        kvStore2.put(1.0f, value);
      } else {
        kvStore2.put("key", value);
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
        kvStore2.put(key, 1.0f);
      } else {
        kvStore2.put(key, "value");
      }
      throw new AssertionError("Expected ClassCastException because the value is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void indicatesValueReplaced() throws LegalSPITesterException {
    Store<K, V> store = factory.newStore();

    K key = factory.createKey(42L);
    V value = factory.createValue(42L);
    V newValue = factory.createValue(256L);

    try {
      store.put(key, value);
      Store.PutStatus putStatus = store.put(key, newValue);
      assertThat(putStatus, is(Store.PutStatus.UPDATE));
      assertThat(store.get(key), notNullValue());
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void indicatesValueReplacedWhenUpdateExpires() throws LegalSPITesterException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    Store<K, V> store = factory.newStoreWithExpiry(Expirations.builder().setUpdate(Duration.ZERO).build(), timeSource);

    K key = factory.createKey(42L);
    V value = factory.createValue(42L);
    V newValue = factory.createValue(256L);

    try {
      store.put(key, value);
      Store.PutStatus putStatus = store.put(key, newValue);
      assertThat(putStatus, is(Store.PutStatus.UPDATE));
      assertThat(store.get(key), nullValue());
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }

  }

  @SPITest
  public void indicatesOperationNoOp() throws LegalSPITesterException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    Store<K, V> store = factory.newStoreWithExpiry(Expirations.builder().setCreate(Duration.ZERO).build(), timeSource);

    K key = factory.createKey(42L);
    try {
      Store.PutStatus putStatus = store.put(key, factory.createValue(42L));
      assertThat(putStatus, is(Store.PutStatus.NOOP));
      assertThat(store.get(key), nullValue());
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }
}
