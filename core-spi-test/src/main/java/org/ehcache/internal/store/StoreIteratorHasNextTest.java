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
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Test the {@link Store.Iterator#hasNext()} contract of the
 * {@link Store.Iterator Store.Iterator} interface.
 *
 * @author Aurelien Broszniowski
 */

public class StoreIteratorHasNextTest<K, V> extends SPIStoreTester<K, V> {

  public StoreIteratorHasNextTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  protected Store<K, V> kvStore;

  @After
  public void tearDown() {
    if (kvStore != null) {
      factory.close(kvStore);
      kvStore = null;
    }
  }

  @SPITest
  public void hasNext()
      throws IllegalAccessException, InstantiationException, StoreAccessException, LegalSPITesterException {
    kvStore = factory.newStore();

    int nbElements = 3;
    for (int i = 0; i < nbElements; i++) {
      kvStore.put(factory.createKey(1), factory.createValue(1));
    }

    Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iterator = kvStore.iterator();

    for (int i = 0; i < nbElements; i++) {
      assertThat(iterator.hasNext(), is(true));
    }
  }

  @SPITest
  public void hasNextReturnsFalseIfNoElement()
      throws IllegalAccessException, InstantiationException, StoreAccessException, LegalSPITesterException {
    kvStore = factory.newStore();

    Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iterator = kvStore.iterator();

    assertThat(iterator.hasNext(), is(false));
  }
}
