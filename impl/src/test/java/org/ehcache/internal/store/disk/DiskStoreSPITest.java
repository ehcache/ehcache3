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

package org.ehcache.internal.store.disk;

import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.expiry.Expirations;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.store.StoreSPITest;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;

/**
 * Test the {@link org.ehcache.internal.store.disk.DiskStore} compliance to the
 * {@link org.ehcache.spi.cache.Store} contract.
 *
 * @author Ludovic Orban
 */

public class DiskStoreSPITest extends StoreSPITest<String, String> {

  private StoreFactory<String, String> storeFactory;

  @Override
  protected StoreFactory<String, String> getStoreFactory() {
    return storeFactory;
  }

  @Before
  public void setUp() {
    storeFactory = new StoreFactory<String, String>() {

      @Override
      public Store<String, String> newStore(final Store.Configuration<String, String> config) {
        DiskStore<String, String> diskStore = new DiskStore<String, String>(config, "diskStore", SystemTimeSource.INSTANCE);
        diskStore.init();
        return diskStore;
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(final String value) {
        return new DiskStorageFactory.DiskValueHolderImpl<String>(value);
      }

      @Override
      public Store.Provider newProvider() {
        return new DiskStore.Provider();
      }

      @Override
      public Store.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Comparable<Long> capacityConstraint,
          final EvictionVeto<? super String, ? super String> evictionVeto, final EvictionPrioritizer<? super String, ? super String> evictionPrioritizer) {
        return new StoreConfigurationImpl<String, String>(keyType, valueType, capacityConstraint,
            evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader(), Expirations.noExpiration(), new JavaSerializationProvider());
      }

      @Override
      public Class<String> getKeyType() {
        return String.class;
      }

      @Override
      public Class<String> getValueType() {
        return String.class;
      }

      @Override
      public ServiceConfiguration<?>[] getServiceConfigurations() {
        return new ServiceConfiguration[0];
      }

      @Override
      public String createKey(long seed) {
        return new String("" + seed);
      }

      @Override
      public String createValue(long seed) {
        return new String("" + seed);
      }
    };
  }

}
