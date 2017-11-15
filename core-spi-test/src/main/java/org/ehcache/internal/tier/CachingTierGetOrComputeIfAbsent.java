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

import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test the {@link CachingTier#getOrComputeIfAbsent(Object, Function)} contract of the
 * {@link CachingTier CachingTier} interface.
 *
 * @author Aurelien Broszniowski
 */

public class CachingTierGetOrComputeIfAbsent<K, V> extends CachingTierTester<K, V> {

  private CachingTier<K, V> tier;

  public CachingTierGetOrComputeIfAbsent(final CachingTierFactory<K, V> factory) {
    super(factory);
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
    if (tier != null) {
      factory.disposeOf(tier);
      tier = null;
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void returnTheValueHolderNotInTheCachingTier() throws LegalSPITesterException {
    K key = factory.createKey(1);
    V value = factory.createValue(1);

    final Store.ValueHolder<V> computedValueHolder = mock(Store.ValueHolder.class);
    when(computedValueHolder.value()).thenReturn(value);

    tier = factory.newCachingTier(1L);

    try {
      Store.ValueHolder<V> valueHolder = tier.getOrComputeIfAbsent(key, new Function<K, Store.ValueHolder<V>>() {
        @Override
        public Store.ValueHolder<V> apply(final K k) {
          return computedValueHolder;
        }
      });

      assertThat(valueHolder.value(), is(equalTo(value)));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void returnTheValueHolderCurrentlyInTheCachingTier() throws LegalSPITesterException {
    K key = factory.createKey(1);
    V value = factory.createValue(1);
    final Store.ValueHolder<V> computedValueHolder = mock(Store.ValueHolder.class);
    when(computedValueHolder.value()).thenReturn(value);
    when(computedValueHolder.expirationTime(any(TimeUnit.class))).thenReturn(Store.ValueHolder.NO_EXPIRE);

    tier = factory.newCachingTier();

    try {
      tier.getOrComputeIfAbsent(key, new Function<K, Store.ValueHolder<V>>() {   // actually put mapping in tier
        @Override
        public Store.ValueHolder<V> apply(final K o) {
          return computedValueHolder;
        }
      });

      Store.ValueHolder<V> valueHolder = tier.getOrComputeIfAbsent(key, new Function<K, Store.ValueHolder<V>>() {
        @Override
        public Store.ValueHolder<V> apply(final K k) {
          return null;
        }
      });

      assertThat(valueHolder.value(), is(equalTo(value)));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

}
