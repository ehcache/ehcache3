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

import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.SPITest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

/**
 * Test the {@link org.ehcache.spi.cache.Store.Provider#createStore(org.ehcache.spi.cache.Store.Configuration, org.ehcache.spi.service.ServiceConfiguration[])} contract of the
 * {@link org.ehcache.spi.cache.Store.Provider Store.Provider} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreProviderCreateStoreTest<K, V> extends SPIStoreTester<K, V> {

  public StoreProviderCreateStoreTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void createStore() throws IllegalAccessException, InstantiationException {
    Store.Provider provider = factory.newProvider();

    Store.Configuration<K, V> storeConfig = factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null);
    Store<K, V> store = provider.createStore(storeConfig, factory.getServiceConfigurations());

    assertThat(store, is(notNullValue()));
  }
}
