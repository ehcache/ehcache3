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
import org.ehcache.config.ResourcePools;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.config.persistence.PersistentStoreConfigurationImpl;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.internal.persistence.DefaultLocalPersistenceService;
import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.store.StoreSPITest;
import org.ehcache.internal.store.disk.DiskStorageFactory;
import org.ehcache.internal.store.disk.DiskStore;
import org.ehcache.internal.store.disk.DiskStoreSPITest;
import org.ehcache.internal.store.heap.OnHeapStore;
import org.ehcache.internal.store.heap.OnHeapStoreByValueSPITest;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.internal.AssumptionViolatedException;

import java.io.File;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.mockito.Mockito.mock;

/**
 * Test the {@link org.ehcache.internal.store.tiering.CacheStore} compliance to the
 * {@link org.ehcache.spi.cache.Store} contract.
 *
 * @author Ludovic Orban
 */

public class CacheStoreSPITest extends StoreSPITest<String, String> {

  private StoreFactory<String, String> storeFactory;
  private final CacheStore.Provider provider = new CacheStore.Provider();
  private final Map<Store<String, String>, String> createdStores = new ConcurrentHashMap<Store<String, String>, String>();
  final LocalPersistenceService localPersistenceService = new DefaultLocalPersistenceService(
          new PersistenceConfiguration(new File(System.getProperty("java.io.tmpdir"), "cache-store-spi-test")));



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
        Serializer<String> keySerializer = new JavaSerializer<String>(config.getClassLoader());
        Serializer<String> valueSerializer = new JavaSerializer<String>(config.getClassLoader());
        Serializer<DiskStorageFactory.Element> elementSerializer = new JavaSerializer<DiskStorageFactory.Element>(config.getClassLoader());
        Serializer<Serializable> objectSerializer = new JavaSerializer<Serializable>(config.getClassLoader());

        OnHeapStore<String, String> onHeapStore = new OnHeapStore<String, String>(config, SystemTimeSource.INSTANCE, false, keySerializer, valueSerializer);
        Store.PersistentStoreConfiguration<String, String, String> persistentStoreConfiguration = (Store.PersistentStoreConfiguration) config;
        try {
          FileBasedPersistenceContext persistenceContext = localPersistenceService.createPersistenceContext(persistentStoreConfiguration.getIdentifier(), persistentStoreConfiguration);
          DiskStore<String, String> diskStore = new DiskStore<String, String>(config, persistenceContext,
                  SystemTimeSource.INSTANCE, elementSerializer, objectSerializer);

          CacheStore<String, String> cacheStore = new CacheStore<String, String>(onHeapStore, diskStore);
          provider.registerStore(cacheStore, new CachingTier.Provider() {
            @Override
            public <K, V> CachingTier<K, V> createCachingTier(final Store.Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
              throw new UnsupportedOperationException("Implement me!");
            }

            @Override
            public void releaseCachingTier(final CachingTier<?, ?> resource) {
              OnHeapStoreByValueSPITest.closeStore((OnHeapStore)resource);
            }

            @Override
            public void initCachingTier(final CachingTier<?, ?> resource) {
              // no op
            }

            @Override
            public void start(final ServiceConfiguration<?> config, final ServiceProvider serviceProvider) {
              throw new UnsupportedOperationException("Implement me!");
            }

            @Override
            public void stop() {
              throw new UnsupportedOperationException("Implement me!");
            }
          }, new AuthoritativeTier.Provider() {
            @Override
            public <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(final Store.Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
              throw new UnsupportedOperationException("Implement me!");
            }

            @Override
            public void releaseAuthoritativeTier(final AuthoritativeTier<?, ?> resource) {
              DiskStoreSPITest.closeStore((DiskStore<?, ?>)resource);
            }

            @Override
            public void initAuthoritativeTier(final AuthoritativeTier<?, ?> resource) {
              DiskStoreSPITest.initStore((DiskStore<?, ?>)resource);
            }

            @Override
            public void start(final ServiceConfiguration<?> config, final ServiceProvider serviceProvider) {
              throw new UnsupportedOperationException("Implement me!");
            }

            @Override
            public void stop() {
              throw new UnsupportedOperationException("Implement me!");
            }
          });
          provider.initStore(cacheStore);
          createdStores.put(cacheStore, persistentStoreConfiguration.getIdentifier());
          return cacheStore;
        } catch (CachePersistenceException e) {
          throw new RuntimeException("Error creation persistence context", e);
        }
      }

      @Override
      public Store<String, String> newStore(Store.Configuration<String, String> config, TimeSource timeSource) {
        SerializationProvider serializationProvider = new DefaultSerializationProvider();
        Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, config.getClassLoader());
        Serializer<String> valueSerializer = serializationProvider.createValueSerializer(String.class, config.getClassLoader());
        Serializer<DiskStorageFactory.Element> elementSerializer = serializationProvider.createValueSerializer(DiskStorageFactory.Element.class, config.getClassLoader());
        Serializer<Serializable> objectSerializer = serializationProvider.createValueSerializer(Serializable.class, config.getClassLoader());

        OnHeapStore<String, String> onHeapStore = new OnHeapStore<String, String>(config, SystemTimeSource.INSTANCE, false, keySerializer, valueSerializer);
        Store.PersistentStoreConfiguration<String, String, String> persistentStoreConfiguration = (Store.PersistentStoreConfiguration) config;
        try {
          FileBasedPersistenceContext persistenceContext = localPersistenceService.createPersistenceContext(persistentStoreConfiguration.getIdentifier(), persistentStoreConfiguration);
          DiskStore<String, String> diskStore = new DiskStore<String, String>(config, persistenceContext,
                  SystemTimeSource.INSTANCE, elementSerializer, objectSerializer);

          CacheStore<String, String> cacheStore = new CacheStore<String, String>(onHeapStore, diskStore);
          provider.initStore(cacheStore);
          createdStores.put(cacheStore, persistentStoreConfiguration.getIdentifier());
          return cacheStore;
        } catch (CachePersistenceException e) {
          throw new RuntimeException("Error creating persistence context", e);
        }
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
          public long expirationTime(TimeUnit unit) {
            return 0;
          }

          @Override
          public boolean isExpired(long expirationTime, TimeUnit unit) {
            return false;
          }

          @Override
          public long lastAccessTime(TimeUnit unit) {
            return 0;
          }

          @Override
          public float hitRate(TimeUnit unit) {
            return 0;
          }

          @Override
          public long getId() {
            throw new UnsupportedOperationException("Implement me!");
          }
        };
      }

      @Override
      public Store.Provider newProvider() {
        Store.Provider provider = new CacheStore.Provider();
        ServiceLocator serviceLocator = new ServiceLocator();
        serviceLocator.addService(new FakeCachingTierProvider());
        serviceLocator.addService(new FakeAuthoritativeTierProvider());
        provider.start(null, serviceLocator);
        return provider;
      }

      @Override
      public Store.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Comparable<Long> capacityConstraint,
          final EvictionVeto<? super String, ? super String> evictionVeto, final EvictionPrioritizer<? super String, ? super String> evictionPrioritizer) {
        StoreConfigurationImpl<String, String> storeConfiguration = new StoreConfigurationImpl<String, String>(keyType, valueType,
                evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader(), Expirations.noExpiration(), buildResourcePools(capacityConstraint));
        return new PersistentStoreConfigurationImpl<String, String>(storeConfiguration, "alias-" + aliasCounter.getAndIncrement());
      }

      @Override
      public Store.Configuration<String, String> newConfiguration(Class<String> keyType, Class<String> valueType, Comparable<Long> capacityConstraint, EvictionVeto<? super String, ? super String> evictionVeto, EvictionPrioritizer<? super String, ? super String> evictionPrioritizer, Expiry<? super String, ? super String> expiry) {
        return new StoreConfigurationImpl<String, String>(keyType, valueType,
            evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader(), expiry, buildResourcePools(capacityConstraint));
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
        return new ServiceConfiguration[]{new CacheStoreServiceConfiguration().cachingTierProvider(FakeCachingTierProvider.class).authoritativeTierProvider(FakeAuthoritativeTierProvider.class)};
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
        String alias = createdStores.get(store);
        provider.releaseStore(store);
        try {
          localPersistenceService.destroyPersistenceContext(alias);
        } catch (CachePersistenceException e) {
          // Nothing to do here
        }
        createdStores.remove(store);
      }

      @Override
      public ServiceProvider getServiceProvider() {
        ServiceLocator serviceLocator = new ServiceLocator();
        serviceLocator.addService(new FakeCachingTierProvider());
        serviceLocator.addService(new FakeAuthoritativeTierProvider());
        return serviceLocator;
      }
    };
  }

  @After
  public void tearDown() throws CachePersistenceException {
    for (Map.Entry<Store<String, String>, String> entry : createdStores.entrySet()) {
      provider.releaseStore(entry.getKey());
      localPersistenceService.destroyPersistenceContext(entry.getValue());
    }
  }

  private ResourcePools buildResourcePools(Comparable<Long> capacityConstraint) {
    if (capacityConstraint == null) {
      return newResourcePoolsBuilder().heap(Long.MAX_VALUE, EntryUnit.ENTRIES).disk(Long.MAX_VALUE, EntryUnit.ENTRIES).build();
    } else {
      return newResourcePoolsBuilder().heap((Long) capacityConstraint, EntryUnit.ENTRIES).disk((Long) capacityConstraint, EntryUnit.ENTRIES).build();
    }
  }

  public static class FakeCachingTierProvider implements CachingTier.Provider {
    @Override
    public <K, V> CachingTier<K, V> createCachingTier(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return mock(CachingTier.class);
    }

    @Override
    public void releaseCachingTier(CachingTier<?, ?> resource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void initCachingTier(CachingTier<?, ?> resource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void stop() {
      throw new UnsupportedOperationException();
    }
  }

  public static class FakeAuthoritativeTierProvider implements AuthoritativeTier.Provider {
    @Override
    public <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return mock(AuthoritativeTier.class);
    }

    @Override
    public void releaseAuthoritativeTier(AuthoritativeTier<?, ?> resource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void initAuthoritativeTier(AuthoritativeTier<?, ?> resource) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void stop() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void testStoreEventListener() {
    throw new AssumptionViolatedException("disabled - EventListeners not implemented yet see #273");
  }

}
