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

package org.ehcache.spi.test;

import org.ehcache.Cache;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Predicates;
import org.ehcache.spi.cache.Store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Test the {@link org.ehcache.spi.cache.Store#close()} contract of the
 * {@link org.ehcache.spi.cache.Store Store} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreCloseTest<K, V> extends SPIStoreTester<K, V> {

  public StoreCloseTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void closedStoreCantBeUsed()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null));

    kvStore.close();

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();

    kvStore.put(key, value);
    // TODO : should an operation throw an exception after the store being closed?
  }

  @SPITest
  public void dataInReopenedClosedStoreCanBeAccessed()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();

    kvStore.put(key, value);

    kvStore.close();

    kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null));

    assertThat(kvStore.containsKey(key), is(true));
  }
}
