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

import static org.ehcache.spi.cache.Store.ValueHolder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test the {@link org.ehcache.spi.cache.Store#get(Object)} contract of the
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
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction
        .all(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();

    kvStore.put(key, value);

    try {
      assertThat(kvStore.get(key), is(instanceOf(ValueHolder.class)));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    } finally {
      if(kvStore != null) {
        kvStore.close();
      }
    }
  }

  @SPITest
  public void keyNotMappedInStoreReturnsNull()
      throws IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    K key = factory.getKeyType().newInstance();

    try {
      assertThat(kvStore.get(key), is(nullValue()));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    } finally {
      if(kvStore != null) {
        kvStore.close();
      }
    }
  }

  @SPITest
  public void existingKeyMappedInStoreReturnsCorrectValueHolder()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction.all(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();

    kvStore.put(key, value);

    try {
      assertThat(kvStore.get(key).value(), is(equalTo(value)));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    } finally {
      if(kvStore != null) {
        kvStore.close();
      }
    }
  }

  @SPITest
  public void nullKeyThrowsException()
      throws IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    K key = null;

    try {
      kvStore.get(key);
      throw new AssertionError("Expected NullPointerException because the key is null");
    } catch (NullPointerException e) {
      // expected
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    } finally {
      if(kvStore != null) {
        kvStore.close();
      }
    }
  }

  @SPITest
  // @Ignore(reason = "Failing test to fix")
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void wrongKeyTypeThrowsException()
      throws IllegalAccessException, InstantiationException {
    final Store kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    try {
      if (this.factory.getKeyType() == String.class) {
        kvStore.get(1.0f);
      } else {
        kvStore.get("key");
      }
      throw new AssertionError("Expected ClassCastException because the key is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    } finally {
      if(kvStore != null) {
        kvStore.close();
      }
    }
  }
}
