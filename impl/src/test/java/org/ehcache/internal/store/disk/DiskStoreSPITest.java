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
import org.ehcache.config.ResourcePools;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.store.StoreSPITest;
import org.ehcache.internal.store.disk.DiskStorageFactory.Element;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;
import org.junit.internal.AssumptionViolatedException;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;

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
        return newStore(config, SystemTimeSource.INSTANCE);
      }

      @Override
      public Store<String, String> newStore(final Store.Configuration<String, String> config, final TimeSource timeSource) {
        SerializationProvider serializationProvider = new JavaSerializationProvider();
        Serializer<Element> elementSerializer = serializationProvider.createSerializer(Element.class, config.getClassLoader());
        Serializer<Object> objectSerializer = serializationProvider.createSerializer(Object.class, config.getClassLoader());
        
        DiskStore<String, String> diskStore = new DiskStore<String, String>(config, "diskStore", timeSource, elementSerializer, objectSerializer);
        try {
          diskStore.destroy();
          diskStore.create();
        } catch (CacheAccessException e) {
          throw new RuntimeException(e);
        }
        diskStore.init();
        return diskStore;
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(final String value) {
        return new DiskStorageFactory.DiskValueHolderImpl<String>(value, SystemTimeSource.INSTANCE.getTimeMillis(), -1);
      }

      @Override
      public Store.Provider newProvider() {
        Store.Provider service = new DiskStore.Provider();
        service.start(null, new ServiceLocator());
        return service;
      }

      @Override
      public Store.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Comparable<Long> capacityConstraint,
          final EvictionVeto<? super String, ? super String> evictionVeto, final EvictionPrioritizer<? super String, ? super String> evictionPrioritizer) {
        return newConfiguration(keyType, valueType, capacityConstraint, evictionVeto, evictionPrioritizer, Expirations.noExpiration());
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
          return newResourcePoolsBuilder().with("disk", "count", "" + Long.MAX_VALUE).build();
        } else {
          return newResourcePoolsBuilder().with("disk", "count", "" + capacityConstraint).build();
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
      public ServiceProvider getServiceProvider() {
        return new ServiceLocator();
      }
    };
  }

  @Override
  public void testDestroy() throws Exception {
    throw new AssumptionViolatedException("disabled - SPITest bug");
  }

  @Override
  public void testStoreEventListener() {
    throw new AssumptionViolatedException("disabled - EventListeners not implemented yet see #273");
  }
}
