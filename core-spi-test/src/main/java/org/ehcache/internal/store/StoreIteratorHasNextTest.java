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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Test the {@link org.ehcache.spi.cache.Store.Iterator#hasNext()} contract of the
 * {@link org.ehcache.spi.cache.Store.Iterator Store.Iterator} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreIteratorHasNextTest<K, V> extends SPIStoreTester<K, V> {

  public StoreIteratorHasNextTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void hasNext()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null));

    int nbElements = 3;
    for (int i = 0; i < nbElements; i++) {
      kvStore.put(factory.getKeyType().newInstance(), factory.getValueType().newInstance());
    }

    Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iterator = kvStore.iterator();

    for (int i = 0; i < nbElements; i++) {
      try {
        assertThat(iterator.hasNext(), is(true));
      } catch (CacheAccessException e) {
        System.err.println("Warning, an exception is thrown due to the SPI test");
        e.printStackTrace();
      }
    }
  }

  @SPITest
  public void hasNextReturnsFalseIfNoElement()
      throws IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null));

    Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iterator = kvStore.iterator();

    try {
      assertThat(iterator.hasNext(), is(false));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }
}
