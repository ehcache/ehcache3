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
import org.ehcache.core.spi.store.Store.ReplaceStatus;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;


/**
 * Test the {@link Store#replace(Object, Object, Object)} contract of the
 * {@link Store Store} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreReplaceKeyValueValueTest<K, V> extends SPIStoreTester<K, V> {

  public StoreReplaceKeyValueValueTest(final StoreFactory<K, V> factory) {
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
  public void replaceCorrectKeyAndValue()
      throws IllegalAccessException, InstantiationException, StoreAccessException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = factory.createKey(1);
    V originalValue = factory.createValue(1);

    kvStore.put(key, originalValue);

    V newValue = factory.createValue(2);

    try {
      kvStore.replace(key, originalValue, newValue);
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }

    assertThat(kvStore.get(key).value(), is(equalTo(newValue)));
  }

  @SPITest
  public void replaceCorrectKeyAndWrongValue()
      throws IllegalAccessException, InstantiationException, StoreAccessException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = factory.createKey(1L);
    V originalValue = factory.createValue(1L);

    kvStore.put(key, originalValue);

    V wrongValue = factory.createValue(2L);
    V newValue = factory.createValue(3L);

    try {
      kvStore.replace(key, wrongValue, newValue);
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }

    assertThat(kvStore.get(key).value(), is(not(equalTo(wrongValue))));
  }

  @SPITest
  public void successfulReplaceReturnsHit()
      throws IllegalAccessException, InstantiationException, StoreAccessException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = factory.createKey(1);
    V originalValue = factory.createValue(1);

    kvStore.put(key, originalValue);

    V newValue = factory.createValue(2);

    try {
      assertThat(kvStore.replace(key, originalValue, newValue), is(ReplaceStatus.HIT));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void unsuccessfulReplaceReturnsMiss()
      throws IllegalAccessException, InstantiationException, StoreAccessException, LegalSPITesterException {
    kvStore = factory.newStore();

    K key = factory.createKey(1);
    V originalValue = factory.createValue(1L);

    kvStore.put(key, originalValue);

    V wrongValue = factory.createValue(2L);
    V newValue = factory.createValue(3L);

    try {
      assertThat(kvStore.replace(key, wrongValue, newValue), is(ReplaceStatus.MISS_PRESENT));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void wrongKeyTypeThrowsException()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore2 = factory.newStore();

    V originalValue = factory.createValue(1);
    V newValue = factory.createValue(2);

    try {
      if (this.factory.getKeyType() == String.class) {
        kvStore2.replace(1.0f, originalValue);
      } else {
        kvStore2.replace("key", originalValue, newValue);
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
  public void wrongOriginalValueTypeThrowsException()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore2 = factory.newStore();

    K key = factory.createKey(1);
    V newValue = factory.createValue(1);

    try {
      if (this.factory.getValueType() == String.class) {
        kvStore2.replace(key, 1.0f, newValue);
      } else {
        kvStore2.replace(key, "value", newValue);
      }
      throw new AssertionError("Expected ClassCastException because the value is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void wrongNewValueTypeThrowsException()
      throws IllegalAccessException, InstantiationException, LegalSPITesterException {
    kvStore2 = factory.newStore();

    K key = factory.createKey(1);
    V originalValue = factory.createValue(1);

    try {
      if (this.factory.getValueType() == String.class) {
        kvStore2.replace(key, originalValue, 1.0f);
      } else {
        kvStore2.replace(key, originalValue, "value");
      }
      throw new AssertionError("Expected ClassCastException because the value is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }
}
