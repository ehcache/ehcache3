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

import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.core.spi.store.Store;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;


import java.time.Duration;

import static org.ehcache.core.spi.store.Store.ValueHolder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test the {@link Store#get(Object)} contract of the
 * {@link Store Store} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreGetTest<K, V> extends SPIStoreTester<K, V> {

  public StoreGetTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  protected Store<K, V> kvStore;

  @After
  public void tearDown() {
    if (kvStore != null) {
      factory.close(kvStore);
      kvStore = null;
    }
  }

  @SPITest
  public void existingKeyMappedInStoreReturnsValueHolder()
      throws IllegalAccessException, InstantiationException, StoreAccessException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = factory.createKey(1);
    V value = factory.createValue(1);

    kvStore.put(key, value);

    try {
      assertThat(kvStore.get(key), is(instanceOf(ValueHolder.class)));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void keyNotMappedInStoreReturnsNull()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = factory.createKey(1L);

    try {
      assertThat(kvStore.get(key), is(nullValue()));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void existingKeyMappedInStoreReturnsCorrectValueHolder()
      throws IllegalAccessException, InstantiationException, StoreAccessException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = factory.createKey(1);
    V value = factory.createValue(1);

    kvStore.put(key, value);

    try {
      assertThat(kvStore.get(key).get(), is(equalTo(value)));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void nullKeyThrowsException()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = null;

    try {
      kvStore.get(key);
      throw new AssertionError("Expected NullPointerException because the key is null");
    } catch (NullPointerException e) {
      // expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void wrongKeyTypeThrowsException()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore = factory.newStore();

    try {
      if (this.factory.getKeyType() == String.class) {
        kvStore.get((K) (Float) 1.0f);
      } else {
        kvStore.get((K) "key");
      }
      throw new AssertionError("Expected ClassCastException because the key is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testGetExpiresOnAccess() throws LegalSPITesterException {
    TestTimeSource timeSource = new TestTimeSource(10043L);
    kvStore = factory.newStoreWithExpiry(ExpiryPolicyBuilder.expiry()
      .access(Duration.ZERO).build(), timeSource);

    K key = factory.createKey(250928L);
    V value = factory.createValue(2059820L);

    try {
      kvStore.put(key, value);
      assertThat(kvStore.get(key).get(), is(value));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }
}
