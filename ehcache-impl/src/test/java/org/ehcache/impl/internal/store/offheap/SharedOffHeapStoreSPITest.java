/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.store.shared.authoritative.AuthoritativeTierPartition;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.tier.AuthoritativeTierFactory;
import org.ehcache.internal.tier.AuthoritativeTierSPITest;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Before;

import java.util.Arrays;
import java.util.Objects;

import static org.ehcache.core.spi.ServiceLocator.dependencySet;

/**
 * SharedOffHeapStoreSPITest
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SharedOffHeapStoreSPITest extends AuthoritativeTierSPITest<String, String> {

  private AuthoritativeTierFactory<String, String> authoritativeTierFactory;
  private SharedOffHeapStoreTest.SharedOffHeapStore sharedStore;
  private AuthoritativeTierPartition<String, String> storePartition;

  @Before
  public void setUp() {
    authoritativeTierFactory = new AuthoritativeTierFactory<String, String>() {
      @Override
      public AuthoritativeTier<String, String> newStore() {
        return newStore(SystemTimeSource.INSTANCE, null, Eviction.noAdvice(), ExpiryPolicyBuilder.noExpiration());
      }

      @Override
      public AuthoritativeTier<String, String> newStoreWithCapacity(long capacity) {
        return newStore(SystemTimeSource.INSTANCE, capacity, Eviction.noAdvice(), ExpiryPolicyBuilder.noExpiration());
      }

      @Override
      public AuthoritativeTier<String, String> newStoreWithExpiry(ExpiryPolicy<? super String, ? super String> expiry, TimeSource timeSource) {
        return newStore(timeSource, null, Eviction.noAdvice(), expiry);
      }

      @Override
      public AuthoritativeTier<String, String> newStoreWithEvictionAdvisor(EvictionAdvisor<String, String> evictionAdvisor) {
        return newStore(SystemTimeSource.INSTANCE, null, evictionAdvisor, ExpiryPolicyBuilder.noExpiration());
      }

      private AuthoritativeTier<String, String> newStore(TimeSource timeSource, Long capacity, EvictionAdvisor evictionAdvisor, ExpiryPolicy<? super String, ? super String> expiry) {
        try {
          Objects.requireNonNull(evictionAdvisor);
          Objects.requireNonNull(expiry);
          sharedStore = new SharedOffHeapStoreTest.SharedOffHeapStore(timeSource, capacity, null);
          storePartition = sharedStore.createAuthoritativeTierPartition(1, String.class, String.class, Eviction.noAdvice(), expiry);
          return storePartition;
        } catch (UnsupportedTypeException e) {
          throw new AssertionError(e);
        }
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
      public ServiceConfiguration<?, ?>[] getServiceConfigurations() {
        return new ServiceConfiguration<?, ?>[0];
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
        if (sharedStore != null) {
          sharedStore.destroy(storePartition);
        }
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
}
