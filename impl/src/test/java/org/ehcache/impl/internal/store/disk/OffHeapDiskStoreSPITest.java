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

package org.ehcache.impl.internal.store.disk;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.core.internal.store.StoreConfigurationImpl;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.CachePersistenceException;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.impl.internal.executor.OnDemandExecutionService;
import org.ehcache.impl.internal.persistence.TestDiskResourceService;
import org.ehcache.impl.internal.store.offheap.BasicOffHeapValueHolder;
import org.ehcache.impl.internal.store.offheap.OffHeapValueHolder;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.impl.serialization.JavaSerializer;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.tier.AuthoritativeTierFactory;
import org.ehcache.internal.tier.AuthoritativeTierSPITest;
import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.persistence.PersistableResourceService.PersistenceSpaceIdentifier;
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
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.core.internal.service.ServiceLocator.dependencySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * OffHeapStoreSPITest
 */
public class OffHeapDiskStoreSPITest extends AuthoritativeTierSPITest<String, String> {

  private AuthoritativeTierFactory<String, String> authoritativeTierFactory;
  private final Map<Store<String, String>, String> createdStores = new ConcurrentHashMap<Store<String, String>, String>();

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public final TestDiskResourceService diskResourceService = new TestDiskResourceService();

  @Before
  public void setUp() throws Exception {
    authoritativeTierFactory = new AuthoritativeTierFactory<String, String>() {

      final AtomicInteger index = new AtomicInteger();

      @Override
      public AuthoritativeTier<String, String> newStore() {
        return newStore(null, null, Expirations.noExpiration(), SystemTimeSource.INSTANCE);
      }

      @Override
      public AuthoritativeTier<String, String> newStoreWithCapacity(long capacity) {
        return newStore(capacity, null, Expirations.noExpiration(), SystemTimeSource.INSTANCE);
      }

      @Override
      public AuthoritativeTier<String, String> newStoreWithExpiry(Expiry<? super String, ? super String> expiry, TimeSource timeSource) {
        return newStore(null, null, expiry, timeSource);
      }

      @Override
      public AuthoritativeTier<String, String> newStoreWithEvictionAdvisor(EvictionAdvisor<String, String> evictionAdvisor) {
        return newStore(null, evictionAdvisor, Expirations.noExpiration(), SystemTimeSource.INSTANCE);
      }


      private AuthoritativeTier<String, String> newStore(Long capacity, EvictionAdvisor<String, String> evictionAdvisor, Expiry<? super String, ? super String> expiry, TimeSource timeSource) {
        Serializer<String> keySerializer = new JavaSerializer<String>(getClass().getClassLoader());
        Serializer<String> valueSerializer = new JavaSerializer<String>(getClass().getClassLoader());

        try {
          CacheConfiguration cacheConfiguration = mock(CacheConfiguration.class);
          when(cacheConfiguration.getResourcePools()).thenReturn(newResourcePoolsBuilder().disk(1, MemoryUnit.MB, false).build());
          String spaceName = "OffheapDiskStore-" + index.getAndIncrement();
          PersistenceSpaceIdentifier space = diskResourceService.getPersistenceSpaceIdentifier(spaceName, cacheConfiguration);
          ResourcePools resourcePools = getDiskResourcePool(capacity);
          SizedResourcePool diskPool = resourcePools.getPoolForResource(DISK);
          MemoryUnit unit = (MemoryUnit)diskPool.getUnit();

          Store.Configuration<String, String> config = new StoreConfigurationImpl<String, String>(getKeyType(), getValueType(),
              evictionAdvisor, getClass().getClassLoader(), expiry, resourcePools, 0, keySerializer, valueSerializer);
          OffHeapDiskStore<String, String> store = new OffHeapDiskStore<String, String>(
                  diskResourceService.createPersistenceContextWithin(space, "store"),
                  new OnDemandExecutionService(), null, 1,
                  config, timeSource,
                  new TestStoreEventDispatcher<String, String>(),
                  unit.toBytes(diskPool.getSize()));
          OffHeapDiskStore.Provider.init(store);
          createdStores.put(store, spaceName);
          return store;
        } catch (CachePersistenceException cpex) {
          throw new RuntimeException("Error creating persistence context", cpex);
        }
      }

      private ResourcePools getDiskResourcePool(Comparable<Long> capacityConstraint) {
        if (capacityConstraint == null) {
          capacityConstraint = 10L;
        }
        return newResourcePoolsBuilder().disk((Long) capacityConstraint, MemoryUnit.MB).build();
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(String value) {
        return new BasicOffHeapValueHolder<String>(-1, value, SystemTimeSource.INSTANCE.getTimeMillis(), OffHeapValueHolder.NO_EXPIRE);
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
        try {
          CacheConfiguration cacheConfiguration = mock(CacheConfiguration.class);
          when(cacheConfiguration.getResourcePools()).thenReturn(newResourcePoolsBuilder().disk(1, MemoryUnit.MB, false).build());
          String spaceName = "OffheapDiskStore-" + index.getAndIncrement();
          PersistenceSpaceIdentifier space = diskResourceService.getPersistenceSpaceIdentifier(spaceName, cacheConfiguration);
          return new ServiceConfiguration[] {space};
        } catch (CachePersistenceException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public ServiceLocator getServiceProvider() {
        ServiceLocator serviceLocator = dependencySet().build();
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
        String spaceName = createdStores.get(store);
        try {
          OffHeapDiskStore.Provider.close((OffHeapDiskStore<String, String>)store);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
        try {
          diskResourceService.destroy(spaceName);
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
        OffHeapDiskStore.Provider.close((OffHeapDiskStore<String, String>) entry.getKey());
        diskResourceService.destroy(entry.getValue());
      }
    } finally {
      diskResourceService.stop();
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
