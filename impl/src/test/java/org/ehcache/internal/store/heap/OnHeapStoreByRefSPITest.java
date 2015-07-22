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

package org.ehcache.internal.store.heap;

import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.store.StoreSPITest;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;

/**
 * Test the {@link org.ehcache.internal.store.heap.OnHeapStore} compliance to the
 * {@link org.ehcache.spi.cache.Store} contract.
 *
 * @author Aurelien Broszniowski
 */

public class OnHeapStoreByRefSPITest extends StoreSPITest<String, String> {

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
        return new OnHeapStore<String, String>(config, SystemTimeSource.INSTANCE, false, null, null);
      }

      @Override
      public Store<String, String> newStore(final Store.Configuration<String, String> config, TimeSource timeSource) {
        return new OnHeapStore<String, String>(config, timeSource, false, null, null);
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(final String value) {
        return new ByRefOnHeapValueHolder<String>(value, SystemTimeSource.INSTANCE.getTimeMillis());
      }

      @Override
      public Store.Provider newProvider() {
        OnHeapStore.Provider service = new OnHeapStore.Provider();
        service.start(getServiceProvider());
        return service;
      }

      @Override
      public Store.Configuration<String, String> newConfiguration(final Class<String> keyType, final Class<String> valueType, final Comparable<Long> capacityConstraint, final EvictionVeto<? super String, ? super String> evictionVeto, final EvictionPrioritizer<? super String, ? super String> evictionPrioritizer) {
        return new StoreConfigurationImpl<String, String>(keyType, valueType,
            evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader(), Expirations.noExpiration(), buildResourcePools(capacityConstraint));
      }

      @Override
      public Store.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Comparable<Long> capacityConstraint,
          final EvictionVeto<? super String, ? super String> evictionVeto, final EvictionPrioritizer<? super String, ? super String> evictionPrioritizer,
          final Expiry<? super String, ? super String> expiry) {
        return new StoreConfigurationImpl<String, String>(keyType, valueType,
            evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader(), expiry, buildResourcePools(capacityConstraint));
      }

      private ResourcePools buildResourcePools(Comparable<Long> capacityConstraint) {
        if (capacityConstraint == null) {
          return newResourcePoolsBuilder().heap(Long.MAX_VALUE, EntryUnit.ENTRIES).build();
        } else {
          return newResourcePoolsBuilder().heap((Long)capacityConstraint, EntryUnit.ENTRIES).build();
        }
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

      @Override
      public void close(final Store<String, String> store) {
        OnHeapStore.Provider.close((OnHeapStore)store);
      }

      @Override
      public ServiceProvider getServiceProvider() {
        return new ServiceLocator();
      }
    };
  }

}
