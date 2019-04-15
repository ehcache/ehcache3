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

package org.ehcache.internal.tier;

import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * CachingTierInvalidate
 */
public class CachingTierInvalidate<K, V> extends CachingTierTester<K, V> {

  private CachingTier<K, V> tier;

  public CachingTierInvalidate(CachingTierFactory<K, V> factory) {
    super(factory);
  }

  @After
  public void tearDown() {
    if (tier != null) {
      factory.disposeOf(tier);
      tier = null;
    }
  }

  @SPITest
  public void invalidateKey() throws LegalSPITesterException {
    tier = factory.newCachingTier();

    final K key = factory.createKey(42L);
    final V value = factory.createValue(42L);

    try {
      // install value
      tier.getOrComputeIfAbsent(key, k -> wrap(value));

      // register invalidation listener
      final AtomicBoolean invalidated = new AtomicBoolean(false);
      tier.setInvalidationListener((key1, valueHolder) -> {
        assertThat(valueHolder.get(), is(value));
        invalidated.set(true);
      });

      tier.invalidate(key);

      assertThat(invalidated.get(), is(true));

    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning - exception thrown", e);
    }
  }

  @SPITest
  public void invalidateTriggeredByEviction() throws LegalSPITesterException {
    tier = factory.newCachingTier(5L);


    try {
      // register invalidation listener
      final AtomicBoolean invalidated = new AtomicBoolean(false);
      tier.setInvalidationListener((key, valueHolder) -> invalidated.set(true));

      // install values
      for (int i = 0; i < 20; i++) {
        final K key = factory.createKey(i^3);
        final V value = factory.createValue(i^5);
        tier.getOrComputeIfAbsent(key, k -> wrap(value));
      }

      assertThat(invalidated.get(), is(true));

    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning - exception thrown", e);
    }
  }

  @SPITest
  public void invalidateAll() throws LegalSPITesterException {
    tier = factory.newCachingTier(20L);


    try {
      // register invalidation listener
      final Set<K> invalidatedKeys = new HashSet<>();
      tier.setInvalidationListener((key, valueHolder) -> invalidatedKeys.add(key));

      // install values
      final Set<K> generatedKeys = new HashSet<>();
      for (int i = 0; i < 5; i++) {
        final K key = factory.createKey(i^3);
        final V value = factory.createValue(i^5);
        generatedKeys.add(key);
        tier.getOrComputeIfAbsent(key, k -> wrap(value));
      }

      tier.invalidateAll();

      assertThat(invalidatedKeys, is(generatedKeys));

    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning - exception thrown", e);
    }
  }

  private Store.ValueHolder<V> wrap(final V value) {
    return new Store.ValueHolder<V>() {
      @Override
      public V get() {
        return value;
      }

      @Override
      public long creationTime() {
        return 0L;
      }

      @Override
      public long expirationTime() {
        return 0L;
      }

      @Override
      public boolean isExpired(long expirationTime) {
        return false;
      }

      @Override
      public long lastAccessTime() {
        return 0L;
      }

      @Override
      public long getId() {
        return -1L;
      }
    };
  }

}
