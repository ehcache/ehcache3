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
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.tier.AuthoritativeTierFactory;
import org.ehcache.internal.tier.AuthoritativeTierSPITest;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;
import org.junit.internal.AssumptionViolatedException;

import java.util.Collections;

/**
 * OffHeapStoreSPITest
 */
public class OffHeapStoreSPITest extends AuthoritativeTierSPITest<String, String> {

  private AuthoritativeTierFactory<String, String> authoritativeTierFactory;

  @Before
  public void setUp() {
    authoritativeTierFactory = new AuthoritativeTierFactory<String, String>() {
      @Override
      public AuthoritativeTier<String, String> newStore(final AuthoritativeTier.Configuration<String, String> config) {
        return newStore(config, SystemTimeSource.INSTANCE);
      }

      @Override
      public AuthoritativeTier<String, String> newStore(final AuthoritativeTier.Configuration<String, String> config, final TimeSource timeSource) {
        Serializer<String> keySerializer = new JavaSerializer<String>(config.getClassLoader());
        Serializer<String> valueSerializer = new JavaSerializer<String>(config.getClassLoader());
        OffHeapStore<String, String> store = new OffHeapStore<String, String>(config, keySerializer, valueSerializer, timeSource, MemoryUnit.MB.toBytes(1));
        OffHeapStore.Provider.init(store);
        return store;
      }

      @Override
      public AuthoritativeTier.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Comparable<Long> capacityConstraint,
          final EvictionVeto<? super String, ? super String> evictionVeto, final EvictionPrioritizer<? super String, ? super String> evictionPrioritizer) {
        return newConfiguration(keyType, valueType, capacityConstraint, evictionVeto, evictionPrioritizer, Expirations.noExpiration());
      }

      @Override
      public AuthoritativeTier.Configuration<String, String> newConfiguration(
          final Class<String> keyType, final Class<String> valueType, final Comparable<Long> capacityConstraint,
          final EvictionVeto<? super String, ? super String> evictionVeto, final EvictionPrioritizer<? super String, ? super String> evictionPrioritizer,
          final Expiry<? super String, ? super String> expiry) {
        return new StoreConfigurationImpl<String, String>(keyType, valueType,
            evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader(), expiry,
            getOffHeapResourcePool(capacityConstraint)
        );
      }

      private ResourcePools getOffHeapResourcePool(Comparable<Long> capacityConstraint) {
        if (capacityConstraint == null) {
          capacityConstraint = 1L;
        }
        return ResourcePoolsBuilder.newResourcePoolsBuilder().offheap((Long)capacityConstraint, MemoryUnit.MB).build();
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(String value) {
        return new OffHeapValueHolder<String>(-1, value, SystemTimeSource.INSTANCE.getTimeMillis(), OffHeapValueHolder.NO_EXPIRE);
      }

      @Override
      public Store.Provider newProvider() {
        Store.Provider provider = new OffHeapStore.Provider();
        provider.start(null, getServiceProvider());
        return provider;
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
        ServiceLocator serviceLocator = new ServiceLocator();
        try {
          serviceLocator.startAllServices(Collections.<Service, ServiceConfiguration<?>>emptyMap());
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
        return Long.toString(seed);
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

  @Override
  public void testStoreEvictionEventListener() throws Exception {
    throw new AssumptionViolatedException("Not yet implemented");
  }
}
