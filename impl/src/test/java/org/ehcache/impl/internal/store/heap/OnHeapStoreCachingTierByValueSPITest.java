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

import org.ehcache.config.ResourcePools;
import org.ehcache.core.internal.store.StoreConfigurationImpl;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.expiry.Expirations;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.impl.internal.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.impl.internal.store.heap.holders.SerializedOnHeapValueHolder;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.impl.serialization.JavaSerializer;
import org.ehcache.internal.tier.CachingTierFactory;
import org.ehcache.internal.tier.CachingTierSPITest;
import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;

import static java.lang.ClassLoader.getSystemClassLoader;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

/**
 * This factory instantiates a CachingTier
 *
 * @author Aurelien Broszniowski
 */
public class OnHeapStoreCachingTierByValueSPITest extends CachingTierSPITest<String, String> {

  private CachingTierFactory<String, String> cachingTierFactory;

  @Override
  protected CachingTierFactory<String, String> getCachingTierFactory() {
    return cachingTierFactory;
  }

  @Before
  public void setUp() {
    cachingTierFactory = new CachingTierFactory<String, String>() {

      final Serializer<String> defaultSerializer = new JavaSerializer<String>(getClass().getClassLoader());
      final Copier<String> defaultCopier = new SerializingCopier<String>(defaultSerializer);

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
                ClassLoader.getSystemClassLoader(), Expirations.noExpiration(), buildResourcePools(capacity), 0,
                new JavaSerializer<String>(getSystemClassLoader()), new JavaSerializer<String>(getSystemClassLoader()));

        return new OnHeapStore<String, String>(config, SystemTimeSource.INSTANCE, defaultCopier, defaultCopier, new NoopSizeOfEngine(), NullStoreEventDispatcher.<String, String>nullStoreEventDispatcher());
      }

      @Override
      public Store.ValueHolder<String> newValueHolder(final String value) {
        return new SerializedOnHeapValueHolder<String>(value, SystemTimeSource.INSTANCE.getTimeMillis(), false, defaultSerializer);
      }

      @Override
      public Store.Provider newProvider() {
        Store.Provider service = new OnHeapStore.Provider();
        service.start(new ServiceLocator());
        return service;
      }

      private ResourcePools buildResourcePools(Comparable<Long> capacityConstraint) {
        if (capacityConstraint == null) {
          return newResourcePoolsBuilder().heap(Long.MAX_VALUE, EntryUnit.ENTRIES).build();
        } else {
          return newResourcePoolsBuilder().heap((Long)capacityConstraint, EntryUnit.ENTRIES).build();
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
