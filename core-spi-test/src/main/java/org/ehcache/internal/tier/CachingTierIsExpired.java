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

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.function.Function;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.SPITest;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test the {@link org.ehcache.spi.cache.tiering.CachingTier#isExpired(Store.ValueHolder)} contract of the
 * {@link org.ehcache.spi.cache.tiering.CachingTier CachingTier} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */
public class CachingTierIsExpired<K, V> extends CachingTierTester<K, V> {

  private CachingTier tier;

  public CachingTierIsExpired(final CachingTierFactory<K, V> factory) {
    super(factory);
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
    if (tier != null) {
      tier.close();
      tier = null;
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void checkThatTheValueHolderExpired() {
    K key = factory.createKey(1);
    V value = factory.createValue(1);

    final Store.ValueHolder<V> computedValueHolder = mock(Store.ValueHolder.class);
    when(computedValueHolder.value()).thenReturn(value);

    TestTimeSource timeSource = new TestTimeSource();
    tier = factory.newCachingTier(factory.newConfiguration(factory.getKeyType(), factory.getValueType(),
        1L, null, null, Expirations.timeToIdleExpiration(new Duration(1, TimeUnit.MILLISECONDS))), timeSource);

    try {
      Store.ValueHolder<V> valueHolder = tier.getOrComputeIfAbsent(key, new Function<K, Store.ValueHolder<V>>() {
        @Override
        public Store.ValueHolder<V> apply(final K k) {
          return computedValueHolder;
        }
      });

      timeSource.advanceTime(1);

      assertThat(tier.isExpired(valueHolder), is(equalTo(true)));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void checkThatTheValueHolderIsNotExpired() {
    K key = factory.createKey(1);
    V value = factory.createValue(1);

    final Store.ValueHolder<V> computedValueHolder = mock(Store.ValueHolder.class);
    when(computedValueHolder.value()).thenReturn(value);

    TestTimeSource timeSource = new TestTimeSource();
    tier = factory.newCachingTier(factory.newConfiguration(factory.getKeyType(), factory.getValueType(),
        1L, null, null, Expirations.timeToIdleExpiration(new Duration(1, TimeUnit.MILLISECONDS))), timeSource);

    try {
      Store.ValueHolder<V> valueHolder = tier.getOrComputeIfAbsent(key, new Function<K, Store.ValueHolder<V>>() {
        @Override
        public Store.ValueHolder<V> apply(final K k) {
          return computedValueHolder;
        }
      });

      assertThat(tier.isExpired(valueHolder), is(equalTo(false)));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void exceptionWhenValueHolderIsNotAnInstanceFromTheCachingTier() {
    Store.ValueHolder<V> valueHolder = mock(Store.ValueHolder.class);

    tier = factory.newCachingTier(factory.newConfiguration(factory.getKeyType(), factory.getValueType(),
        1L, null, null, Expirations.noExpiration()));

    try {
      tier.isExpired(valueHolder);
      throw new AssertionError();
    } catch (ClassCastException e) {
      // expected
    }
  }
}
