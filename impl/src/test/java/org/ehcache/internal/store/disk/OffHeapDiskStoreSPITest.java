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
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.persistence.PersistentStoreConfigurationImpl;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.internal.persistence.DefaultLocalPersistenceService;
import org.ehcache.internal.persistence.TestLocalPersistenceService;
import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.store.offheap.OffHeapValueHolder;
import org.ehcache.internal.tier.AuthoritativeTierFactory;
import org.ehcache.internal.tier.AuthoritativeTierSPITest;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.test.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.config.ResourceType.Core.DISK;

/**
 * OffHeapStoreSPITest
 */
public class OffHeapDiskStoreSPITest extends AuthoritativeTierSPITest<String, String> {

  private AuthoritativeTierFactory<String, String> authoritativeTierFactory;
  private final Map<Store<String, String>, String> createdStores = new ConcurrentHashMap<Store<String, String>, String>();

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();
  
  @Rule
  public final TestLocalPersistenceService persistenceService = new TestLocalPersistenceService();
  
  @Before
  public void setUp() throws Exception {
    authoritativeTierFactory = new AuthoritativeTierFactory<String, String>() {

      final AtomicInteger index = new AtomicInteger();

      @Override
      public AuthoritativeTier<String, String> newStore(final AuthoritativeTier.Configuration<String, String> config) {
        return newStore(config, SystemTimeSource.INSTANCE);
      }

      @Override
      public AuthoritativeTier<String, String> newStore(final AuthoritativeTier.Configuration<String, String> config, final TimeSource timeSource) {
        Serializer<String> keySerializer = new JavaSerializer<String>(config.getClassLoader());
        Serializer<String> valueSerializer = new JavaSerializer<String>(config.getClassLoader());

        Store.PersistentStoreConfiguration<String, String, String> persistentStoreConfiguration = (Store.PersistentStoreConfiguration) config;
        try {
          FileBasedPersistenceContext persistenceContext = persistenceService.createPersistenceContext(persistentStoreConfiguration
              .getIdentifier(), persistentStoreConfiguration);

          ResourcePool pool = config.getResourcePools().getPoolForResource(DISK);
          MemoryUnit unit = (MemoryUnit)pool.getUnit();

          OffHeapDiskStore<String, String> store = new OffHeapDiskStore<String, String>(persistenceContext, config, keySerializer, valueSerializer, timeSource, unit.toBytes(pool.getSize()));
          OffHeapDiskStore.Provider.init(store);
          createdStores.put(store, persistentStoreConfiguration.getIdentifier());
          return store;
        } catch (CachePersistenceException cpex) {
          throw new RuntimeException("Error creating persistence context", cpex);
        }
      }

      @Override
      public AuthoritativeTier.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Long capacityConstraint,
          final EvictionVeto<? super String, ? super String> evictionVeto, final EvictionPrioritizer<? super String, ? super String> evictionPrioritizer) {
        return newConfiguration(keyType, valueType, capacityConstraint, evictionVeto, evictionPrioritizer, Expirations.noExpiration());
      }

      @Override
      public AuthoritativeTier.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Long capacityConstraint,
          final EvictionVeto<? super String, ? super String> evictionVeto, final EvictionPrioritizer<? super String, ? super String> evictionPrioritizer,
          final Expiry<? super String, ? super String> expiry) {
        StoreConfigurationImpl<String, String> storeConfiguration = new StoreConfigurationImpl<String, String>(keyType, valueType,
            evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader(), expiry,
            getDiskResourcePool(capacityConstraint)
        );
        return new PersistentStoreConfigurationImpl<String, String>(storeConfiguration, "offheapdiskStore-" + index.getAndIncrement());
      }

      private ResourcePools getDiskResourcePool(Comparable<Long> capacityConstraint) {
        if (capacityConstraint == null) {
          capacityConstraint = 32L;
        }
        return ResourcePoolsBuilder.newResourcePoolsBuilder().disk((Long) capacityConstraint, MemoryUnit.MB).build();
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(String value) {
        return new OffHeapValueHolder<String>(-1, value, SystemTimeSource.INSTANCE.getTimeMillis(), OffHeapValueHolder.NO_EXPIRE);
      }

      @Override
      public Store.Provider newProvider() {
        Store.Provider provider = new OffHeapDiskStore.Provider();
        try {
          LocalPersistenceService localPersistenceService = new DefaultLocalPersistenceService(new CacheManagerPersistenceConfiguration(folder.newFolder()));
          localPersistenceService.start(null);
          ServiceLocator serviceProvider = getServiceProvider();
          serviceProvider.addService(localPersistenceService);
          provider.start(serviceProvider);
          return provider;
        } catch (IOException ex) {
          throw new AssertionError(ex);
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
      public ServiceLocator getServiceProvider() {
        ServiceLocator serviceLocator = new ServiceLocator();
        try {
          serviceLocator.startAllServices();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return serviceLocator;
      }

      @Override
      public String createKey(long seed) {
        return Long.toString(seed);
      }

      @Override
      public String createValue(long seed) {
        char[] chars = new char[400 * 1024];
        Arrays.fill(chars, (char) (0x1 + (seed & 0x7e)));
        return new String(chars);
      }

      @Override
      public void close(final Store<String, String> store) {
        String alias = createdStores.get(store);
        try {
          OffHeapDiskStore.Provider.close((OffHeapDiskStore)store);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
        try {
          persistenceService.destroyPersistenceContext(alias);
        } catch (CachePersistenceException ex) {
          throw new AssertionError(ex);
        } finally {
          createdStores.remove(store);
        }
      }
    };
  }

  @After
  public void tearDown() throws CachePersistenceException, IOException {
    try {
      for (Map.Entry<Store<String, String>, String> entry : createdStores.entrySet()) {
        OffHeapDiskStore.Provider.close((OffHeapDiskStore) entry.getKey());
        persistenceService.destroyPersistenceContext(entry.getValue());
      }
    } finally {
      persistenceService.stop();
    }
  }

  public static void initStore(final OffHeapDiskStore<?, ?> diskStore) {
    OffHeapDiskStore.Provider.init(diskStore);
  }

  public static void closeStore(final OffHeapDiskStore<?, ?> diskStore) {
    try {
      OffHeapDiskStore.Provider.close(diskStore);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected AuthoritativeTierFactory<String, String> getAuthoritativeTierFactory() {
    return authoritativeTierFactory;
  }

  @Override
  protected StoreFactory<String, String> getStoreFactory() {
    return getAuthoritativeTierFactory();
  }
}
