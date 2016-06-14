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
import org.ehcache.core.internal.store.StoreConfigurationImpl;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Expirations;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.impl.internal.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.DefaultSizeOfEngine;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.heap.holders.CopiedOnHeapValueHolder;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.internal.tier.CachingTierFactory;
import org.ehcache.internal.tier.CachingTierSPITest;
import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

public class OnHeapStoreCachingTierByRefSPITest extends CachingTierSPITest<String, String> {

  private CachingTierFactory<String, String> cachingTierFactory;

  @Override
  protected CachingTierFactory<String, String> getCachingTierFactory() {
    return cachingTierFactory;
  }

  @Before
  public void setUp() {
    cachingTierFactory = new CachingTierFactory<String, String>() {

      private final Copier DEFAULT_COPIER = new IdentityCopier();

      @Override
      public CachingTier<String, String> newCachingTier() {
        return newCachingTier(null);
      }

      @Override
      public CachingTier<String, String> newCachingTier(long capacity) {
        return newCachingTier((Long) capacity);
      }

      private CachingTier<String, String> newCachingTier(Long capacity) {
        Store.Configuration<String, String> config = new StoreConfigurationImpl<String, String>(getKeyType(), getValueType(), null,
                ClassLoader.getSystemClassLoader(), Expirations.noExpiration(), buildResourcePools(capacity), 0, null, null);

        return new OnHeapStore<String, String>(config, SystemTimeSource.INSTANCE, DEFAULT_COPIER, DEFAULT_COPIER,
            new DefaultSizeOfEngine(Long.MAX_VALUE, Long.MAX_VALUE), NullStoreEventDispatcher.<String, String>nullStoreEventDispatcher());
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(final String value) {
        return new CopiedOnHeapValueHolder<String>(value, SystemTimeSource.INSTANCE.getTimeMillis(), false, DEFAULT_COPIER);
      }

      @Override
      public Store.Provider newProvider() {
        return new OnHeapStore.Provider();
      }

      private ResourcePools buildResourcePools(Comparable<Long> capacityConstraint) {
        if (capacityConstraint == null) {
          return newResourcePoolsBuilder().heap(10l, MemoryUnit.MB).build();
        } else {
          return newResourcePoolsBuilder().heap((Long)capacityConstraint, MemoryUnit.MB).build();
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
      public void disposeOf(CachingTier tier) {
      }

      @Override
      public ServiceProvider<Service> getServiceProvider() {
        return new ServiceLocator();
      }

    };
  }

}
