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

import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Test the {@link Store#clear()} contract of the
 * {@link Store Store} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreClearTest<K, V> extends SPIStoreTester<K, V> {

  public StoreClearTest(final StoreFactory<K, V> factory) {
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
  public void removesAllOfTheMappings()
      throws IllegalAccessException, InstantiationException, StoreAccessException, LegalSPITesterException {
    K key = factory.createKey(42);
    V value = factory.createValue(42);

    kvStore.put(key, value);

    try {
      kvStore.clear();
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }

    assertThat(kvStore.containsKey(key), is(false));
  }
}
