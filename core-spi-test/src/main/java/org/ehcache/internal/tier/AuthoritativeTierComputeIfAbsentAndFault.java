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
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test the {@link AuthoritativeTier#computeIfAbsentAndFault(Object, Function)} contract of the
 * {@link AuthoritativeTier AuthoritativeTier} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class AuthoritativeTierComputeIfAbsentAndFault<K, V> extends SPIAuthoritativeTierTester<K, V> {

  protected AuthoritativeTier<K, V> tier;

  public AuthoritativeTierComputeIfAbsentAndFault(final AuthoritativeTierFactory<K, V> factory) {
    super(factory);
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
    if (tier != null) {
//      tier.close();
      tier = null;
    }
  }

  /**
   * The goal of this test is to make sure that an entry that was calculated by the computeIfAbsent method
   * will be evicted with the default behaviour of the tier.
   */
  @SPITest
  public void nonMarkedMappingIsEvictable() throws StoreAccessException {
    K key = factory.createKey(1);

    tier = factory.newStoreWithCapacity(1L);

    tier.computeIfAbsent(key, new Function<K, V>() {
      @Override
      public V apply(final K k) {
        return factory.createValue(1L);
      }
    });

    fillTierOverCapacity(tier, factory);

    assertThat(tier.get(key), is(nullValue()));
  }

  /**
   * This test depends on the previous test, while the previous tests verifies that the eviction will occur,
   * this one will verify that the eviction doesn't occur under the same condition after a call to computeIfAbsentAndFault()
   */
  @SPITest
  public void marksTheMappingAsNotEvictableAndComputeValue() throws LegalSPITesterException {
    K key = factory.createKey(1);
    V value = factory.createValue(1);

    tier = factory.newStoreWithCapacity(1L);

    try {
      assertThat(tier.get(key), is(nullValue()));
      assertThat(tier.computeIfAbsentAndFault(key, new Function<K, V>() {
        @Override
        public V apply(final K k) {
          return factory.createValue(1L);
        }
      }).value(), is(equalTo(value)));

      fillTierOverCapacity(tier, factory);
      assertThat(tier.get(key).value(), is(equalTo(value)));

    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  private void fillTierOverCapacity(AuthoritativeTier<K, V> tier, AuthoritativeTierFactory<K, V> factory) throws StoreAccessException {
    for (long seed = 2L; seed < 10; seed++) {
      tier.put(factory.createKey(seed), factory.createValue(seed));
    }
  }
}
