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

import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.spi.test.After;
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
 * Test the {@link AuthoritativeTier#flush(Object, Store.ValueHolder)} contract of the
 * {@link AuthoritativeTier AuthoritativeTier} interface.
 *
 * @author Aurelien Broszniowski
 */

public class AuthoritativeTierFlush<K, V> extends SPIAuthoritativeTierTester<K, V> {

  protected AuthoritativeTier<K, V> tier;

  public AuthoritativeTierFlush(final AuthoritativeTierFactory<K, V> factory) {
    super(factory);
  }

  @After
  public void tearDown() {
    if (tier != null) {
      factory.close(tier);
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void entryIsFlushed() throws LegalSPITesterException {
    K key = factory.createKey(1);
    final V value = factory.createValue(1);
    Store.ValueHolder<V> valueHolder = mock(Store.ValueHolder.class);
    when(valueHolder.expirationTime()).thenReturn(1L);

    tier = factory.newStoreWithCapacity(1L);

    try {
      tier.put(key, value);
      final Store.ValueHolder<V> fault = tier.getAndFault(key);
      when(valueHolder.getId()).thenReturn(fault.getId());
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }

    assertThat(tier.flush(key, valueHolder), is(equalTo(true)));
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void entryIsNotFlushed() throws LegalSPITesterException {
    K key = factory.createKey(1);
    final V value = factory.createValue(1);
    Store.ValueHolder<V> valueHolder = mock(Store.ValueHolder.class);
    when(valueHolder.expirationTime()).thenReturn(1L);

    tier = factory.newStoreWithCapacity(1L);

    try {
      tier.put(key, value);
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }

    assertThat(tier.flush(key, valueHolder), is(equalTo(false)));
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void entryDoesNotExist() {
    K key = factory.createKey(1);
    Store.ValueHolder<V> valueHolder = mock(Store.ValueHolder.class);
    when(valueHolder.expirationTime()).thenReturn(1L);

    tier = factory.newStoreWithCapacity(1L);

    assertThat(tier.flush(key, valueHolder), is(equalTo(false)));
  }

}
