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

package org.ehcache.impl.internal.store.heap.bytesized;

import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.DefaultSizeOfEngine;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.heap.holders.SerializedOnHeapValueHolder;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.impl.serialization.JavaSerializer;
import org.ehcache.internal.tier.CachingTierFactory;
import org.ehcache.internal.tier.CachingTierSPITest;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;

import java.util.Arrays;

import static java.lang.ClassLoader.getSystemClassLoader;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.core.spi.ServiceLocator.dependencySet;

public class OnHeapStoreCachingTierByValueSPITest extends CachingTierSPITest<String, String> {

  private CachingTierFactory<String, String> cachingTierFactory;

  @Override
  protected CachingTierFactory<String, String> getCachingTierFactory() {
    return cachingTierFactory;
  }

  @Before
  public void setUp() {
    cachingTierFactory = new CachingTierFactory<String, String>() {

      final Serializer<String> defaultSerializer = new JavaSerializer<>(getClass().getClassLoader());
      final Copier<String> defaultCopier = new SerializingCopier<>(defaultSerializer);

      @Override
      public CachingTier<String, String> newCachingTier() {
        return newCachingTier(null);
      }

      @Override
      public CachingTier<String, String> newCachingTier(long capacity) {
        return newCachingTier((Long) capacity);
      }

      private CachingTier<String, String> newCachingTier(Long capacity) {
        Store.Configuration<String, String> config = new StoreConfigurationImpl<>(getKeyType(), getValueType(), null,
          ClassLoader.getSystemClassLoader(), ExpiryPolicyBuilder.noExpiration(), buildResourcePools(capacity), 0,
          new JavaSerializer<>(getSystemClassLoader()), new JavaSerializer<>(getSystemClassLoader()));
        return new OnHeapStore<>(config, SystemTimeSource.INSTANCE, defaultCopier, defaultCopier,
          new DefaultSizeOfEngine(Long.MAX_VALUE, Long.MAX_VALUE), NullStoreEventDispatcher.nullStoreEventDispatcher());
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(final String value) {
        return new SerializedOnHeapValueHolder<>(value, SystemTimeSource.INSTANCE.getTimeMillis(), false, defaultSerializer);
      }

      @Override
      public Store.Provider newProvider() {
        Store.Provider service = new OnHeapStore.Provider();
        service.start(dependencySet().build());
        return service;
      }

      private ResourcePools buildResourcePools(Comparable<Long> capacityConstraint) {
        if (capacityConstraint == null) {
          capacityConstraint = 10L;
        }
        return newResourcePoolsBuilder().heap((Long)capacityConstraint, MemoryUnit.MB).build();
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
        return Long.toString(seed);
      }

      @Override
      public String createValue(long seed) {
        char[] chars = new char[600 * 1024];
        Arrays.fill(chars, (char) (0x1 + (seed & 0x7e)));
        return new String(chars);
      }

      @Override
      public void disposeOf(CachingTier<String, String> tier) {
      }

      @Override
      public ServiceProvider<Service> getServiceProvider() {
        return dependencySet().build();
      }

    };
  }

}
