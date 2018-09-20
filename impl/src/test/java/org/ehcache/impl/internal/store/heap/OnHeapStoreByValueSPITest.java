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

package org.ehcache.impl.internal.store.heap;

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.impl.internal.store.heap.holders.SerializedOnHeapValueHolder;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.impl.serialization.JavaSerializer;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.store.StoreSPITest;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;

import static java.lang.ClassLoader.getSystemClassLoader;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.core.spi.ServiceLocator.dependencySet;

/**
 * Test the {@link OnHeapStore} compliance to the
 * {@link Store} contract.
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

      final Serializer<String> defaultSerializer = new JavaSerializer<>(getClass().getClassLoader());
      final Copier<String> defaultCopier = new SerializingCopier<>(defaultSerializer);

      @Override
      public Store<String, String> newStore() {
        return newStore(null, null, ExpiryPolicyBuilder.noExpiration(), SystemTimeSource.INSTANCE);
      }

      @Override
      public Store<String, String> newStoreWithCapacity(long capacity) {
        return newStore(capacity, null, ExpiryPolicyBuilder.noExpiration(), SystemTimeSource.INSTANCE);
      }

      @Override
      public Store<String, String> newStoreWithExpiry(ExpiryPolicy<? super String, ? super String> expiry, TimeSource timeSource) {
        return newStore(null, null, expiry, timeSource);
      }

      @Override
      public Store<String, String> newStoreWithEvictionAdvisor(EvictionAdvisor<String, String> evictionAdvisor) {
        return newStore(null, evictionAdvisor, ExpiryPolicyBuilder.noExpiration(), SystemTimeSource.INSTANCE);
      }

      private Store<String, String> newStore(Long capacity, EvictionAdvisor<String, String> evictionAdvisor, ExpiryPolicy<? super String, ? super String> expiry, TimeSource timeSource) {
        ResourcePools resourcePools = buildResourcePools(capacity);
        Store.Configuration<String, String> config = new StoreConfigurationImpl<>(getKeyType(), getValueType(),
          evictionAdvisor, getClass().getClassLoader(), expiry, resourcePools, 0,
          new JavaSerializer<>(getSystemClassLoader()), new JavaSerializer<>(getSystemClassLoader()));
        return new OnHeapStore<>(config, timeSource, defaultCopier, defaultCopier, new NoopSizeOfEngine(), new TestStoreEventDispatcher<>());
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(final String value) {
        return new SerializedOnHeapValueHolder<>(value, SystemTimeSource.INSTANCE.getTimeMillis(), false, defaultSerializer);
      }

      private ResourcePools buildResourcePools(Comparable<Long> capacityConstraint) {
        if (capacityConstraint == null) {
          return newResourcePoolsBuilder().heap(Long.MAX_VALUE, EntryUnit.ENTRIES).build();
        } else {
          return newResourcePoolsBuilder().heap((Long) capacityConstraint, EntryUnit.ENTRIES).build();
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
        return new ServiceConfiguration<?>[0];
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
      public ServiceLocator getServiceProvider() {
        ServiceLocator serviceLocator = dependencySet().build();
        try {
          serviceLocator.startAllServices();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return serviceLocator;
      }
    };
  }

  public static void closeStore(OnHeapStore<?, ?> store) {
    OnHeapStore.Provider.close(store);
  }

}
