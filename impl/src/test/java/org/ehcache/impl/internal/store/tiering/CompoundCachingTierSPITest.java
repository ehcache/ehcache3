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

package org.ehcache.impl.internal.store.tiering;

import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.offheap.OffHeapStore;
import org.ehcache.impl.internal.store.offheap.OffHeapStoreLifecycleHelper;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.impl.serialization.JavaSerializer;
import org.ehcache.internal.tier.CachingTierFactory;
import org.ehcache.internal.tier.CachingTierSPITest;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;

import java.util.IdentityHashMap;
import java.util.Map;

import static java.lang.ClassLoader.getSystemClassLoader;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.core.spi.ServiceLocator.dependencySet;

/**
 * This factory instantiates a CachingTier
 *
 * @author Ludovic Orban
 */
public class CompoundCachingTierSPITest extends CachingTierSPITest<String, String> {

  private CachingTierFactory<String, String> cachingTierFactory;
  private Map<CompoundCachingTier<?, ?>, OffHeapStore<?, ?>> map = new IdentityHashMap<>();

  @Override
  protected CachingTierFactory<String, String> getCachingTierFactory() {
    return cachingTierFactory;
  }

  @Before
  public void setUp() {
    cachingTierFactory = new CachingTierFactory<String, String>() {

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
          ClassLoader.getSystemClassLoader(), ExpiryPolicyBuilder.noExpiration(), buildResourcePools(capacity), 0, new JavaSerializer<>(getSystemClassLoader()), new JavaSerializer<>(getSystemClassLoader()));

        StoreEventDispatcher<String, String> eventDispatcher = NullStoreEventDispatcher.nullStoreEventDispatcher();
        OffHeapStore<String, String> offHeapStore = new OffHeapStore<>(config, SystemTimeSource.INSTANCE, eventDispatcher, 10 * 1024 * 1024);
        OffHeapStoreLifecycleHelper.init(offHeapStore);
        IdentityCopier<String> copier = new IdentityCopier<>();
        OnHeapStore<String, String> onHeapStore = new OnHeapStore<>(config, SystemTimeSource.INSTANCE, copier, copier, new NoopSizeOfEngine(), eventDispatcher);
        CompoundCachingTier<String, String> compoundCachingTier = new CompoundCachingTier<>(onHeapStore, offHeapStore);
        map.put(compoundCachingTier, offHeapStore);
        return compoundCachingTier;
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(final String value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Store.Provider newProvider() {
        return new OnHeapStore.Provider();
      }

      private ResourcePools buildResourcePools(Comparable<Long> capacityConstraint) {
        if (capacityConstraint == null) {
          return newResourcePoolsBuilder().heap(Long.MAX_VALUE, EntryUnit.ENTRIES).offheap(1, MemoryUnit.MB).build();
        } else {
          return newResourcePoolsBuilder().heap((Long)capacityConstraint, EntryUnit.ENTRIES).offheap(1, MemoryUnit.MB).build();
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
        return "" + seed;
      }

      @Override
      public String createValue(long seed) {
        return "" + seed;
      }

      @Override
      public void disposeOf(CachingTier<String, String> tier) {
        OffHeapStore<?, ?> offHeapStore = map.remove(tier);
        OffHeapStoreLifecycleHelper.close(offHeapStore);
      }

      @Override
      public ServiceProvider<Service> getServiceProvider() {
        return dependencySet().build();
      }

    };
  }

}
