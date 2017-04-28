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

import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.SPITest;
import org.hamcrest.Matchers;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test contract of closing a
 * {@link Store Store} interface.
 *
 * @author Aurelien Broszniowski
 */

public class StoreCloseTest<K, V> extends SPIStoreTester<K, V> {

  public StoreCloseTest(final StoreFactory<K, V> factory) {
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
      kvStore = null;
    }
  }

  @SPITest
  public void closedStoreCantBeUsed()
      throws StoreAccessException, IllegalAccessException, InstantiationException {
    K key = factory.createKey(42);
    V value = factory.createValue(42);

    kvStore.put(key, value);

    factory.close(kvStore);

    try {
      assertThat(kvStore.containsKey(key), Matchers.is(false));
    } catch (Exception e) {
      System.err.println("An exception is thrown, This might happen since the store is closed.");
      e.printStackTrace();
    }
  }
}
