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
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.store.disk.DiskStorageFactory.Element;
import org.ehcache.internal.tier.AuthoritativeTierFactory;
import org.ehcache.internal.tier.AuthoritativeTierSPITest;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.test.After;
import org.junit.Before;
import org.junit.internal.AssumptionViolatedException;

import java.io.File;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;

/**
 * Test the {@link org.ehcache.internal.store.disk.DiskStore} compliance to the
 * {@link org.ehcache.spi.cache.Store} contract.
 *
 * @author Ludovic Orban
 */

public class DiskStoreSPITest extends AuthoritativeTierSPITest<String, String> {

  private AuthoritativeTierFactory<String, String> authoritativeTierFactory;
  private final Map<Store<String, String>, String> createdStores = new ConcurrentHashMap<Store<String, String>, String>();
  private LocalPersistenceService persistenceService;

  @Before
  public void setUp() {
    persistenceService = new DefaultLocalPersistenceService(
        new PersistenceConfiguration(new File(System.getProperty("java.io.tmpdir"), "disk-store-spi-test")));
    authoritativeTierFactory = new AuthoritativeTierFactory<String, String>() {

      final AtomicInteger index = new AtomicInteger();

      @Override
      public AuthoritativeTier<String, String> newStore(final Store.Configuration<String, String> config) {
        return newStore(config, SystemTimeSource.INSTANCE);
      }

      @Override
      public AuthoritativeTier<String, String> newStore(final Store.Configuration<String, String> config, final TimeSource timeSource) {
        SerializationProvider serializationProvider = new DefaultSerializationProvider();
        serializationProvider.start(null, null);
        Serializer<Element> elementSerializer = serializationProvider.createValueSerializer(Element.class, config.getClassLoader());
        Serializer<Serializable> objectSerializer = serializationProvider.createValueSerializer(Serializable.class, config.getClassLoader());

        Store.PersistentStoreConfiguration<String, String, String> persistentStoreConfiguration = (Store.PersistentStoreConfiguration) config;
        try {
          FileBasedPersistenceContext persistenceContext = persistenceService.createPersistenceContext(persistentStoreConfiguration.getIdentifier(), persistentStoreConfiguration);
          DiskStore<String, String> diskStore = new DiskStore<String, String>(config, persistenceContext, timeSource,
              elementSerializer, objectSerializer);
          DiskStore.Provider.init(diskStore);
          createdStores.put(diskStore, persistentStoreConfiguration.getIdentifier());
          return diskStore;
        } catch (CachePersistenceException e) {
          throw new RuntimeException("Error creation persistence context", e);
        }
      }

      @Override
      public AuthoritativeTier.ValueHolder<String> newValueHolder(final String value) {
        return new DiskStorageFactory.DiskValueHolder<String>(-1, value, SystemTimeSource.INSTANCE.getTimeMillis(), -1);
      }

      @Override
      public Store.Provider newProvider() {
        Store.Provider service = new DiskStore.Provider();
        LocalPersistenceService localPersistenceService = new DefaultLocalPersistenceService(
            new PersistenceConfiguration(new File(System.getProperty("java.io.tmpdir"))));
        ServiceLocator serviceProvider = getServiceProvider();
        serviceProvider.addService(localPersistenceService);
        service.start(null, serviceProvider);
        return service;
      }

      @Override
      public AuthoritativeTier.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Comparable<Long> capacityConstraint,
          final EvictionVeto<? super String, ? super String> evictionVeto, final EvictionPrioritizer<? super String, ? super String> evictionPrioritizer) {
        Store.Configuration<String, String> configuration = newConfiguration(keyType, valueType, capacityConstraint, evictionVeto, evictionPrioritizer, Expirations
            .noExpiration());
        return new PersistentStoreConfigurationImpl<String, String>(configuration, "diskStore-" + index.getAndIncrement());
      }

      @Override
      public AuthoritativeTier.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Comparable<Long> capacityConstraint,
          final EvictionVeto<? super String, ? super String> evictionVeto, final EvictionPrioritizer<? super String, ? super String> evictionPrioritizer,
          final Expiry<? super String, ? super String> expiry) {
        StoreConfigurationImpl<String, String> configuration = new StoreConfigurationImpl<String, String>(keyType, valueType,
            evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader(), expiry, buildResourcePools(capacityConstraint));
        return new PersistentStoreConfigurationImpl<String, String>(configuration, "diskStore-" + index.getAndIncrement());
      }

      private ResourcePools buildResourcePools(Comparable<Long> capacityConstraint) {
        if (capacityConstraint == null) {
          return newResourcePoolsBuilder().disk(Long.MAX_VALUE, EntryUnit.ENTRIES).build();
        } else {
          return newResourcePoolsBuilder().disk((Long)capacityConstraint, EntryUnit.ENTRIES).build();
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
        String alias = createdStores.get(store);
        DiskStore.Provider.close((DiskStore)store);
        try {
          persistenceService.destroyPersistenceContext(alias);
        } catch (CachePersistenceException e) {
          // Not doing anything here
        }
        createdStores.remove(store);
      }

      @Override
      public ServiceLocator getServiceProvider() {
        ServiceLocator serviceLocator = new ServiceLocator();
        try {
          serviceLocator.startAllServices(Collections.<Service, ServiceConfiguration<?>>emptyMap());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return serviceLocator;
      }
    };
  }

  @After
  public void tearDown() throws CachePersistenceException {
    for (Map.Entry<Store<String, String>, String> entry : createdStores.entrySet()) {
      DiskStore.Provider.close((DiskStore) entry.getKey());
      persistenceService.destroyPersistenceContext(entry.getValue());
    }
  }

  public static void initStore(final DiskStore<?, ?> diskStore) {
    DiskStore.Provider.init(diskStore);
  }

  public static void closeStore(final DiskStore<?, ?> diskStore) {
    DiskStore.Provider.close(diskStore);
  }

  @Override
  protected AuthoritativeTierFactory<String, String> getAuthoritativeTierFactory() {
    return authoritativeTierFactory;
  }

  @Override
  protected StoreFactory<String, String> getStoreFactory() {
    return getAuthoritativeTierFactory();
  }

  @Override
  public void testStoreExpiryEventListener() throws Exception {
    throw new AssumptionViolatedException("disabled - EventListeners not implemented yet see #273");
  }

  @Override
  public void testStoreEvictionEventListener() throws Exception {
    throw new AssumptionViolatedException("disabled - EventListeners not implemented yet see #273");
  }
}
