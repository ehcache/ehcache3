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
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Predicates;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.SPITest;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * Test the {@link org.ehcache.spi.cache.Store#replace(K key, V value, V value)} contract of the
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
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K,V>>all(), null));

    K key = factory.getKeyType().newInstance();
    V originalValue = factory.getValueType().newInstance();

    kvStore.put(key, originalValue);

    V newValue = factory.getValueType().newInstance();

    kvStore.replace(key, originalValue, newValue);

    assertThat(kvStore.get(key).value(), is(equalTo(newValue)));
  }

  @SPITest
  public void replaceCorrectKeyAndWrongValue()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K,V>>all(), null));

    K key = factory.getKeyType().newInstance();
    V originalValue = factory.getValueType().newInstance();

    kvStore.put(key, originalValue);

    V wrongValue = factory.getValueType().newInstance();
    V newValue = factory.getValueType().newInstance();

    kvStore.replace(key, wrongValue, newValue);

    assertThat(kvStore.get(key).value(), is(not(equalTo(wrongValue))));
  }

  @SPITest
  public void successfulReplaceReturnsTrue()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K,V>>all(), null));

    K key = factory.getKeyType().newInstance();
    V originalValue = factory.getValueType().newInstance();

    kvStore.put(key, originalValue);

    V newValue = factory.getValueType().newInstance();

    boolean successfulReplace = kvStore.replace(key, originalValue, newValue);

    assertThat(successfulReplace, is(true));
  }

  @SPITest
  public void unsuccessfulReplaceReturnsTrue()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K,V>>all(), null));

    K key = factory.getKeyType().newInstance();
    V originalValue = factory.getValueType().newInstance();

    kvStore.put(key, originalValue);

    V wrongValue = factory.getValueType().newInstance();
    V newValue = factory.getValueType().newInstance();

    boolean successfulReplace = kvStore.replace(key, wrongValue, newValue);

    assertThat(successfulReplace, is(false));
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void wrongKeyTypeThrowsException()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType()));

    V originalValue = factory.getValueType().newInstance();
    V newValue = factory.getValueType().newInstance();

    try {
      if (this.factory.getKeyType() == String.class) {
        kvStore.replace(1.0f, originalValue);
      } else {
        kvStore.replace("key", originalValue, newValue);
      }
      fail("Expected ClassCastException because the key is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void wrongOriginalValueTypeThrowsException()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType()));

    K key = factory.getKeyType().newInstance();
    V newValue = factory.getValueType().newInstance();

    try {
      if (this.factory.getKeyType() == String.class) {
        kvStore.replace(key, 1.0f, newValue);
      } else {
        kvStore.replace(key, "value", newValue);
      }
      fail("Expected ClassCastException because the value is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void wrongNewValueTypeThrowsException()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType()));

    K key = factory.getKeyType().newInstance();
    V originalValue = factory.getValueType().newInstance();

    try {
      if (this.factory.getKeyType() == String.class) {
        kvStore.replace(key, originalValue, 1.0f);
      } else {
        kvStore.replace(key, originalValue, "value");
      }
      fail("Expected ClassCastException because the value is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    }
  }

  @SPITest
  public void mappingCantBeReplacedCanThrowException()
      throws IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType()));

    K key = factory.getKeyType().newInstance();
    V originalValue = factory.getValueType().newInstance();
    V newValue = factory.getValueType().newInstance();

    try {
      kvStore.replace(key, originalValue, newValue);
    } catch (CacheAccessException e) {
      // This will not compile if the CacheAccessException is not thrown
    }
  }
}
