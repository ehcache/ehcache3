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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Test the {@link org.ehcache.spi.cache.Store#remove(Object, Object)} contract of the
 * {@link org.ehcache.spi.cache.Store Store} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreRemoveKeyValueTest<K, V> extends SPIStoreTester<K, V> {

  public StoreRemoveKeyValueTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void removeEntryForKeyIfMappedToValue()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction
        .all(), null));

    K key = factory.createKey(1L);
    V value = factory.createValue(1L);

    kvStore.put(key, value);

    K equalKey = factory.createKey(1L);
    V equalValue = factory.createValue(1L);

    assertThat(key.equals(equalKey), is(true));
    assertThat(value.equals(equalValue), is(true));

    try {
      kvStore.remove(equalKey, equalValue);
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }

    assertThat(kvStore.containsKey(key), is(false));
  }

  @SPITest
  public void doNothingForKeyNotMappedToValue()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction.all(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();

    assertThat(kvStore.containsKey(key), is(false));

    try {
      boolean isRemoved = kvStore.remove(key, value);
      assertThat(isRemoved, is(false));
    } catch (CacheAccessException e) {
      throw new AssertionError(e);
    }
  }

  @SPITest
  public void doNothingForWrongValue()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction.all(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.createValue(1L);

    kvStore.put(key, value);

    V notEqualValue = factory.createValue(2L);

    assertThat(value.equals(notEqualValue), is(false));

    try {
      assertThat(kvStore.remove(key, notEqualValue), is(false));
    } catch (CacheAccessException e) {
      throw new AssertionError(e);
    }
  }

  @SPITest
  public void returnTrueIfValueWasRemoved()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction.all(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();

    kvStore.put(key, value);

    assertThat(kvStore.containsKey(key), is(true));

    try {
      assertThat(kvStore.remove(key, value), is(true));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void returnFalseIfValueWasNotRemoved()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction.all(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();

    assertThat(kvStore.containsKey(key), is(false));

    try {
      assertThat(kvStore.remove(key, value), is(false));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void nullKeyThrowsException()
      throws IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    K key = null;
    V value = factory.getValueType().newInstance();

    try {
      kvStore.remove(key, value);
      throw new AssertionError("Expected NullPointerException because the key is null");
    } catch (NullPointerException e) {
      // expected
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void nullValueThrowsException()
      throws IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    K key = factory.getKeyType().newInstance();
    V value = null;

    try {
      kvStore.remove(key, value);
      throw new AssertionError("Expected NullPointerException because the value is null");
    } catch (NullPointerException e) {
      // expected
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void wrongKeyTypeThrowsException()
      throws IllegalAccessException, InstantiationException {
    final Store kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    V value = factory.getValueType().newInstance();

    try {
      if (this.factory.getKeyType() == String.class) {
        kvStore.remove(1.0f, value);
      } else {
        kvStore.remove("key", value);
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
  @SuppressWarnings("unchecked")
  public void wrongValueTypeThrowsException()
      throws IllegalAccessException, InstantiationException {
    final Store kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    K key = factory.getKeyType().newInstance();

    try {
      if (this.factory.getValueType() == String.class) {
        kvStore.remove(key, 1.0f);
      } else {
        kvStore.remove(key, "value");
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