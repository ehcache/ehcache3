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

import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.SPITest;

/**
 * Test the {@link org.ehcache.spi.cache.Store.Provider#releaseStore(org.ehcache.spi.cache.Store)} contract of the
 * {@link org.ehcache.spi.cache.Store.Provider Store.Provider} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreProviderReleaseStoreTest<K, V> extends SPIStoreTester<K, V> {

  public StoreProviderReleaseStoreTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void releaseStore() throws IllegalAccessException, InstantiationException {
    Store.Provider provider = factory.newProvider();

    Store.Configuration<K, V> storeConfig = new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType());
    Store<K, V> store = provider.createStore(storeConfig, factory.getServiceConfigurations());

    provider.releaseStore(store);

    // TODO : javadoc is missing information to know what to assert here (e.g. is exception thrown in case of subsequent operations such as store.get(key)?)
  }
}
