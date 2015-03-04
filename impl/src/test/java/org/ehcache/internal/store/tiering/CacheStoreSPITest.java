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

package org.ehcache.internal.store.tiering;

import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.internal.store.OnHeapStore;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.store.StoreSPITest;
import org.ehcache.internal.store.disk.DiskStorageFactory;
import org.ehcache.internal.store.disk.DiskStore;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;
import org.junit.internal.AssumptionViolatedException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test the {@link org.ehcache.internal.store.tiering.CacheStore} compliance to the
 * {@link org.ehcache.spi.cache.Store} contract.
 *
 * @author Ludovic Orban
 */

public class CacheStoreSPITest extends StoreSPITest<String, String> {

  private StoreFactory<String, String> storeFactory;

  @Override
  protected StoreFactory<String, String> getStoreFactory() {
    return storeFactory;
  }

  @Before
  public void setUp() {
    storeFactory = new StoreFactory<String, String>() {
      final AtomicInteger aliasCounter = new AtomicInteger();

      @Override
      public Store<String, String> newStore(final Store.Configuration<String, String> config) {
        JavaSerializationProvider serializationProvider = new JavaSerializationProvider();
        Serializer<String> keySerializer = serializationProvider.createSerializer(String.class, config.getClassLoader());
        Serializer<String> valueSerializer = serializationProvider.createSerializer(String.class, config.getClassLoader());
        Serializer<DiskStorageFactory.Element> elementSerializer = serializationProvider.createSerializer(DiskStorageFactory.Element.class, config.getClassLoader());
        Serializer<Object> objectSerializer = serializationProvider.createSerializer(Object.class, config.getClassLoader());

        OnHeapStore<String, String> onHeapStore = new OnHeapStore<String, String>(config, SystemTimeSource.INSTANCE, false, keySerializer, valueSerializer);
        DiskStore<String, String> diskStore = new DiskStore<String, String>(config, "alias-" + aliasCounter.incrementAndGet(), SystemTimeSource.INSTANCE, elementSerializer, objectSerializer);

        CacheStore<String, String> cacheStore = new CacheStore<String, String>(onHeapStore, diskStore);
        try {
          cacheStore.destroy();
          cacheStore.create();
        } catch (CacheAccessException e) {
          throw new RuntimeException(e);
        }
        cacheStore.init();
        return cacheStore;
      }

      @Override
      public Store<String, String> newStore(Store.Configuration<String, String> config, TimeSource timeSource) {
        JavaSerializationProvider serializationProvider = new JavaSerializationProvider();
        Serializer<String> keySerializer = serializationProvider.createSerializer(String.class, config.getClassLoader());
        Serializer<String> valueSerializer = serializationProvider.createSerializer(String.class, config.getClassLoader());
        Serializer<DiskStorageFactory.Element> elementSerializer = serializationProvider.createSerializer(DiskStorageFactory.Element.class, config.getClassLoader());
        Serializer<Object> objectSerializer = serializationProvider.createSerializer(Object.class, config.getClassLoader());

        OnHeapStore<String, String> onHeapStore = new OnHeapStore<String, String>(config, SystemTimeSource.INSTANCE, false, keySerializer, valueSerializer);
        DiskStore<String, String> diskStore = new DiskStore<String, String>(config, "alias-" + aliasCounter.incrementAndGet(), SystemTimeSource.INSTANCE, elementSerializer, objectSerializer);

        CacheStore<String, String> cacheStore = new CacheStore<String, String>(onHeapStore, diskStore);
        try {
          cacheStore.destroy();
          cacheStore.create();
        } catch (CacheAccessException e) {
          throw new RuntimeException(e);
        }
        cacheStore.init();
        return cacheStore;
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(final String value) {
        final long creationTime = SystemTimeSource.INSTANCE.getTimeMillis();
        return new Store.ValueHolder<String>() {

          @Override
          public String value() {
            return value;
          }

          @Override
          public long creationTime(TimeUnit unit) {
            return creationTime;
          }

          @Override
          public long lastAccessTime(TimeUnit unit) {
            return 0;
          }

          @Override
          public float hitRate(TimeUnit unit) {
            return 0;
          }
        };
      }

      @Override
      public Store.Provider newProvider() {
        return new CacheStore.Provider();
      }

      @Override
      public Store.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Comparable<Long> capacityConstraint,
          final EvictionVeto<? super String, ? super String> evictionVeto, final EvictionPrioritizer<? super String, ? super String> evictionPrioritizer) {
        return new StoreConfigurationImpl<String, String>(keyType, valueType, capacityConstraint,
            evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader(), Expirations.noExpiration());
      }

      @Override
      public Store.Configuration<String, String> newConfiguration(Class<String> keyType, Class<String> valueType, Comparable<Long> capacityConstraint, EvictionVeto<? super String, ? super String> evictionVeto, EvictionPrioritizer<? super String, ? super String> evictionPrioritizer, Expiry<? super String, ? super String> expiry) {
        return new StoreConfigurationImpl<String, String>(keyType, valueType, capacityConstraint,
            evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader(), expiry);
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
