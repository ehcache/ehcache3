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
 * Test the {@link org.ehcache.spi.cache.Store#clear()} contract of the
 * {@link org.ehcache.spi.cache.Store Store} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreClearTest<K, V> extends SPIStoreTester<K, V> {

  public StoreClearTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void valueHolderCanBeRetrievedWithEqualKey()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K,V>>all(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();

    kvStore.put(key, value);

    kvStore.clear();

    assertThat(kvStore.containsKey(key), is(false));
  }

  @SPITest
  public void storeCantBeClearedCanThrowException()
      throws IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType()));

    try {
      kvStore.clear();
    } catch (CacheAccessException e) {
      // This will not compile if the CacheAccessException is not thrown
    }
  }
}
