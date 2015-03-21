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

package org.ehcache.internal.store.offheap;

import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.store.StoreSPITest;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.internal.AssumptionViolatedException;

/**
 * OffHeapStoreSPITest
 */
public class OffHeapStoreSPITest extends StoreSPITest<String, String> {
  @Override
  protected StoreFactory<String, String> getStoreFactory() {
    return new StoreFactory<String, String>() {
      @Override
      public Store<String, String> newStore(Store.Configuration<String, String> config) {
        return newStore(config, SystemTimeSource.INSTANCE);
      }

      @Override
      public Store<String, String> newStore(Store.Configuration<String, String> config, TimeSource timeSource) {
        JavaSerializationProvider serializationProvider = new JavaSerializationProvider();

        OffHeapStore<String, String> store = new OffHeapStore<String, String>(config, serializationProvider
            .createSerializer(String.class, config.getClassLoader()),
            serializationProvider.createSerializer(String.class, config.getClassLoader()), timeSource, MemoryUnit.MB.toBytes(1));
        store.init();
        return store;
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(String value) {
        return new OffHeapValueHolder<String>(value, SystemTimeSource.INSTANCE.getTimeMillis(), OffHeapValueHolder.NO_EXPIRE);
      }

      @Override
      public Store.Provider newProvider() {
        return new OffHeapStore.Provider();
      }

      @Override
      public Store.Configuration<String, String> newConfiguration(Class<String> keyType, Class<String> valueType, Comparable<Long> capacityConstraint, EvictionVeto<? super String, ? super String> evictionVeto, EvictionPrioritizer<? super String, ? super String> evictionPrioritizer) {
        return newConfiguration(keyType, valueType, capacityConstraint, evictionVeto, evictionPrioritizer, Expirations.noExpiration());
      }

      @Override
      public Store.Configuration<String, String> newConfiguration(Class<String> keyType, Class<String> valueType, Comparable<Long> capacityConstraint, EvictionVeto<? super String, ? super String> evictionVeto, EvictionPrioritizer<? super String, ? super String> evictionPrioritizer, Expiry<? super String, ? super String> expiry) {
        // TODO Wire capacity constraint
        return new StoreConfigurationImpl<String, String>(keyType, valueType,
            evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader(), expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(1, MemoryUnit.MB).build());
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
      public ServiceProvider getServiceProvider() {
        return new ServiceLocator();
      }

      @Override
      public String createKey(long seed) {
        return Long.toString(seed);
      }

      @Override
      public String createValue(long seed) {
        return Long.toString(seed);
      }
    };
  }

  @Override
  public void testStoreEventListener() throws Exception {
    throw new AssumptionViolatedException("Not yet implemented");
  }
}
