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
import org.ehcache.spi.cache.Store;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test the {@link org.ehcache.spi.cache.Store#iterator()} contract of the
 * {@link org.ehcache.spi.cache.Store Store} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreIteratorTest<K, V> extends SPIStoreTester<K, V> {

  public StoreIteratorTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void iterableContainsValuesInAnyOrder()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType()));

    V value1 = factory.getValueType().newInstance();
    V value2 = factory.getValueType().newInstance();
    V value3 = factory.getValueType().newInstance();

    kvStore.put(factory.getKeyType().newInstance(), value1);
    kvStore.put(factory.getKeyType().newInstance(), value2);
    kvStore.put(factory.getKeyType().newInstance(), value3);

    Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iterator = kvStore.iterator();
    List<V> values = new ArrayList<V>();
    while(iterator.hasNext()) {
      Cache.Entry<K, Store.ValueHolder<V>> nextEntry = iterator.next();
      values.add(nextEntry.getValue().value());
    }
    assertThat(values, containsInAnyOrder(equalTo(value1), equalTo(value2), equalTo(value3)));
  }


}
