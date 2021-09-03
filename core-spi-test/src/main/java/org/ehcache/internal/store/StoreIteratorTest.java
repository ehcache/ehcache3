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
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.SPITest;


import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test the {@link Store#iterator()} contract of the
 * {@link Store Store} interface.
 *
 * @author Aurelien Broszniowski
 */

public class StoreIteratorTest<K, V> extends SPIStoreTester<K, V> {

  public StoreIteratorTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  protected Store<K, V> kvStore;

  @Before
  public void setUp() {
    kvStore = factory.newStore();
  }

  @After
  public void tearDown() {
    if (kvStore != null) {
      factory.close(kvStore);
      kvStore = null;
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void iterableContainsValuesInAnyOrder()
      throws StoreAccessException, IllegalAccessException, InstantiationException {
    K key1 = factory.createKey(1L);
    K key2 = factory.createKey(2L);
    K key3 = factory.createKey(3L);
    V value1 = factory.createValue(1L);
    V value2 = factory.createValue(2L);
    V value3 = factory.createValue(3L);

    kvStore.put(key1, value1);
    kvStore.put(key2, value2);
    kvStore.put(key3, value3);

    Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iterator = kvStore.iterator();
    List<K> keys = new ArrayList<>();
    List<V> values = new ArrayList<>();
    while (iterator.hasNext()) {
      Cache.Entry<K, Store.ValueHolder<V>> nextEntry = iterator.next();
      keys.add(nextEntry.getKey());
      values.add(nextEntry.getValue().get());
    }
    assertThat(keys, containsInAnyOrder(equalTo(key1), equalTo(key2), equalTo(key3)));
    assertThat(values, containsInAnyOrder(equalTo(value1), equalTo(value2), equalTo(value3)));
  }
}
