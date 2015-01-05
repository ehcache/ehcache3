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

import org.ehcache.config.Eviction;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.SPITest;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;


/**
 * Test the {@link org.ehcache.spi.cache.Store#replace(Object, Object, Object)} contract of the
 * {@link org.ehcache.spi.cache.Store Store} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreReplaceKeyValueValueTest<K, V> extends SPIStoreTester<K, V> {

  public StoreReplaceKeyValueValueTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void replaceCorrectKeyAndValue()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction
        .all(), null));

    K key = factory.getKeyType().newInstance();
    V originalValue = factory.getValueType().newInstance();

    kvStore.put(key, originalValue);

    V newValue = factory.getValueType().newInstance();

    try {
      kvStore.replace(key, originalValue, newValue);
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }

    assertThat(kvStore.get(key).value(), is(equalTo(newValue)));
  }

  @SPITest
  public void replaceCorrectKeyAndWrongValue()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction.all(), null));

    K key = factory.createKey(1L);
    V originalValue = factory.createValue(1L);

    kvStore.put(key, originalValue);

    V wrongValue = factory.createValue(2L);
    V newValue = factory.createValue(3L);

    try {
      kvStore.replace(key, wrongValue, newValue);
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }

    assertThat(kvStore.get(key).value(), is(not(equalTo(wrongValue))));
  }

  @SPITest
  public void successfulReplaceReturnsTrue()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction.all(), null));

    K key = factory.getKeyType().newInstance();
    V originalValue = factory.getValueType().newInstance();

    kvStore.put(key, originalValue);

    V newValue = factory.getValueType().newInstance();

    try {
      assertThat(kvStore.replace(key, originalValue, newValue), is(true));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void unsuccessfulReplaceReturnsFalse()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction.all(), null));

    K key = factory.getKeyType().newInstance();
    V originalValue = factory.createValue(1L);

    kvStore.put(key, originalValue);

    V wrongValue = factory.createValue(2L);
    V newValue = factory.createValue(3L);

    try {
      assertThat(kvStore.replace(key, wrongValue, newValue), is(false));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void wrongKeyTypeThrowsException()
      throws IllegalAccessException, InstantiationException {
    final Store kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    V originalValue = factory.getValueType().newInstance();
    V newValue = factory.getValueType().newInstance();

    try {
      if (this.factory.getKeyType() == String.class) {
        kvStore.replace(1.0f, originalValue);
      } else {
        kvStore.replace("key", originalValue, newValue);
      }
      throw new AssertionError("Expected ClassCastException because the key is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void wrongOriginalValueTypeThrowsException()
      throws IllegalAccessException, InstantiationException {
    final Store kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    K key = factory.getKeyType().newInstance();
    V newValue = factory.getValueType().newInstance();

    try {
      if (this.factory.getValueType() == String.class) {
        kvStore.replace(key, 1.0f, newValue);
      } else {
        kvStore.replace(key, "value", newValue);
      }
      throw new AssertionError("Expected ClassCastException because the value is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void wrongNewValueTypeThrowsException()
      throws IllegalAccessException, InstantiationException {
    final Store kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    K key = factory.getKeyType().newInstance();
    V originalValue = factory.getValueType().newInstance();

    try {
      if (this.factory.getValueType() == String.class) {
        kvStore.replace(key, originalValue, 1.0f);
      } else {
        kvStore.replace(key, originalValue, "value");
      }
      throw new AssertionError("Expected ClassCastException because the value is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }
}
