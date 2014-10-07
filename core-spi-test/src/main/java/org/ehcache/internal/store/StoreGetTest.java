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

import static org.ehcache.spi.cache.Store.ValueHolder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test the {@link org.ehcache.spi.cache.Store#get(K key)} contract of the
 * {@link org.ehcache.spi.cache.Store Store} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreGetTest<K, V> extends SPIStoreTester<K, V> {

  public StoreGetTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void existingKeyMappedInStoreReturnsValueHolder()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K,V>>all(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();

    kvStore.put(key, value);

    assertThat(kvStore.get(key), is(instanceOf(ValueHolder.class)));
  }

  @SPITest
  public void keyNotMappedInStoreReturnsNull()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(this.factory.getKeyType(), this.factory.getValueType()));

    K key = factory.getKeyType().newInstance();

    assertThat(kvStore.get(key), is(nullValue()));
  }

  @SPITest
  public void existingKeyMappedInStoreReturnsCorrectValueHolder()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K,V>>all(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();

    kvStore.put(key, value);
    ValueHolder<V> returnedValueHolder = kvStore.get(key);

    assertThat(returnedValueHolder.value(), is(equalTo(value)));
  }

  @SPITest
  public void nullKeyThrowsException()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType()));

    K key = null;

    try {
      kvStore.get(key);
      throw new AssertionError("Expected NullPointerException because the key is null");
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void wrongKeyTypeThrowsException()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType()));

    try {
      if (this.factory.getKeyType() == String.class) {
        kvStore.get(1.0f);
      } else {
        kvStore.get("key");
      }
      throw new AssertionError("Expected ClassCastException because the key is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    }
  }

  @SPITest
  public void retrievalCanThrowException()
      throws IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType()));

    K key = factory.getKeyType().newInstance();

    try {
      kvStore.get(key);
    } catch (CacheAccessException e) {
      // This will not compile if the CacheAccessException is not thrown
    }
  }
}