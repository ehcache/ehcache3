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
import org.ehcache.expiry.Expirations;
import org.ehcache.function.Predicate;
import org.ehcache.internal.HeapResourceCacheConfiguration;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.internal.store.service.OnHeapStoreServiceConfig;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;

import java.util.Comparator;

/**
 * Test the {@link org.ehcache.internal.store.OnHeapStore} compliance to the
 * {@link org.ehcache.spi.cache.Store} contract.
 *
 * @author Aurelien Broszniowski
 */

public class OnHeapStoreByValueSPITest extends StoreSPITest<String, String> {

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
        return new OnHeapStore<String, String>(config, SystemTimeSource.INSTANCE, true);
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(final String value) {
        return new TimeStampedOnHeapByValueValueHolder<String>(value,
                new JavaSerializer<String>(), SystemTimeSource.INSTANCE.getTimeMillis(), TimeStampedOnHeapByRefValueHolder.NO_EXPIRE);
      }

      @Override
      public Store.Provider newProvider() {
        return new OnHeapStore.Provider();
      }

      @Override
      public Store.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Comparable<Long> capacityConstraint,
          final Predicate<Cache.Entry<String, String>> evictionVeto, final Comparator<Cache.Entry<String, String>> evictionPrioritizer) {
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
      public ServiceConfiguration[] getServiceConfigurations() {
        return new ServiceConfiguration[] { new HeapResourceCacheConfiguration(100), new OnHeapStoreServiceConfig().storeByValue(true)};
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
