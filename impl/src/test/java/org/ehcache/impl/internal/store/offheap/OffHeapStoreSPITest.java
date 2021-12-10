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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.impl.serialization.JavaSerializer;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.tier.AuthoritativeTierFactory;
import org.ehcache.internal.tier.AuthoritativeTierSPITest;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;

import java.util.Arrays;

import static org.ehcache.config.ResourceType.Core.OFFHEAP;
import static org.ehcache.core.spi.ServiceLocator.dependencySet;

/**
 * OffHeapStoreSPITest
 */
public class OffHeapStoreSPITest extends AuthoritativeTierSPITest<String, String> {

  private AuthoritativeTierFactory<String, String> authoritativeTierFactory;

  @Before
  public void setUp() {
    authoritativeTierFactory = new AuthoritativeTierFactory<String, String>() {
      @Override
      public AuthoritativeTier<String, String> newStore() {
        return newStore(null, null, ExpiryPolicyBuilder.noExpiration(), SystemTimeSource.INSTANCE);
      }

      @Override
      public AuthoritativeTier<String, String> newStoreWithCapacity(long capacity) {
        return newStore(capacity, null, ExpiryPolicyBuilder.noExpiration(), SystemTimeSource.INSTANCE);
      }

      @Override
      public AuthoritativeTier<String, String> newStoreWithExpiry(ExpiryPolicy<? super String, ? super String> expiry, TimeSource timeSource) {
        return newStore(null, null, expiry, timeSource);
      }

      @Override
      public AuthoritativeTier<String, String> newStoreWithEvictionAdvisor(EvictionAdvisor<String, String> evictionAdvisor) {
        return newStore(null, evictionAdvisor, ExpiryPolicyBuilder.noExpiration(), SystemTimeSource.INSTANCE);
      }


      private AuthoritativeTier<String, String> newStore(Long capacity, EvictionAdvisor<String, String> evictionAdvisor, ExpiryPolicy<? super String, ? super String> expiry, TimeSource timeSource) {
        Serializer<String> keySerializer = new JavaSerializer<>(getClass().getClassLoader());
        Serializer<String> valueSerializer = new JavaSerializer<>(getClass().getClassLoader());

        ResourcePools resourcePools = getOffHeapResourcePool(capacity);
        SizedResourcePool offheapPool = resourcePools.getPoolForResource(OFFHEAP);
        MemoryUnit unit = (MemoryUnit)offheapPool.getUnit();

        Store.Configuration<String, String> config = new StoreConfigurationImpl<>(getKeyType(), getValueType(),
          evictionAdvisor, getClass().getClassLoader(), expiry, resourcePools, 0, keySerializer, valueSerializer);
        OffHeapStore<String, String> store = new OffHeapStore<>(config, timeSource, new TestStoreEventDispatcher<>(), unit
          .toBytes(offheapPool.getSize()));
        OffHeapStore.Provider.init(store);
        return store;
      }

      private ResourcePools getOffHeapResourcePool(Comparable<Long> capacityConstraint) {
        if (capacityConstraint == null) {
          capacityConstraint = 10L;
        }
        return ResourcePoolsBuilder.newResourcePoolsBuilder().offheap((Long)capacityConstraint, MemoryUnit.MB).build();
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(String value) {
        return new BasicOffHeapValueHolder<>(-1, value, SystemTimeSource.INSTANCE.getTimeMillis(), OffHeapValueHolder.NO_EXPIRE);
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
        return new ServiceConfiguration<?>[0];
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
        char[] chars = new char[600 * 1024];
        Arrays.fill(chars, (char) (0x1 + (seed & 0x7e)));
        return new String(chars);
      }

      @Override
      public void close(final Store<String, String> store) {
        OffHeapStore.Provider.close((OffHeapStore)store);
      }
    };
  }

  @Override
  protected AuthoritativeTierFactory<String, String> getAuthoritativeTierFactory() {
    return authoritativeTierFactory;
  }

  @Override
  protected StoreFactory<String, String> getStoreFactory() {
    return getAuthoritativeTierFactory();
  }

  public static void initStore(OffHeapStore<?, ?> offHeapStore) {
    OffHeapStore.Provider.init(offHeapStore);
  }

  public static void closeStore(OffHeapStore<?, ?> offHeapStore) {
    OffHeapStore.Provider.close(offHeapStore);
  }

}
