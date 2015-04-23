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
import org.ehcache.spi.Persistable;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.SPITest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Test the {@link Persistable#destroy()} contract of the
 * {@link org.ehcache.spi.cache.Store stores} implementing the interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreDestroyTest<K, V> extends SPIStoreTester<K, V> {

  public StoreDestroyTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  protected Store<K, V> kvStore;

  @Before
  public void setUp() {
    kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, Eviction
        .all(), null));
  }

  @After
  public void tearDown() {
    if (kvStore != null) {
      kvStore.close();
    }
  }

  @SPITest
  public void noDataCanBeRecoveredFromDestroyedStore()
      throws IllegalAccessException, InstantiationException, CacheAccessException {
    if (!(kvStore instanceof Persistable)) {
      System.err.println("disabled - store is not an instance of org.ehcache.spi.Persistable");
      return;
    }

    K key = factory.createKey(1);
    V value = factory.createValue(1);

    kvStore.put(key, value);

    try {
      ((Persistable) kvStore).destroy();
    } catch (Exception e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }

    try {
      assertThat(kvStore.containsKey(key), is(false));
    } catch (Exception e) {
      System.err.println("An exception is thrown, This might happen since the store is destroyed.");
      e.printStackTrace();
    }
  }
}
