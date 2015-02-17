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
import org.ehcache.config.Eviction;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.SPITest;

import java.util.NoSuchElementException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Test the {@link org.ehcache.spi.cache.Store.Iterator#next()} contract of the
 * {@link org.ehcache.spi.cache.Store.Iterator Store.Iterator} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreIteratorNextTest<K, V> extends SPIStoreTester<K, V> {

  public StoreIteratorNextTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void nextReturnsNextElement()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction
        .all(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();
    kvStore.put(key, value);

    Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iterator = kvStore.iterator();

    try {
      Cache.Entry<K, Store.ValueHolder<V>> entry = iterator.next();
      assertThat(entry.getKey(), is(equalTo(key)));
      assertThat(entry.getValue().value(), is(equalTo(value)));
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
  public void noMoreElementThrowsException()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction.all(), null));

    kvStore.put(factory.getKeyType().newInstance(), factory.getValueType().newInstance());

    Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iterator = kvStore.iterator();

    try {
      iterator.next();
      iterator.next();
      throw new AssertionError("Expected NoSuchElementException because no more element could be found");
    } catch (NoSuchElementException e) {
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
