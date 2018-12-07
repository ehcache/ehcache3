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

import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.store.heap.holders.OnHeapValueHolder;
import org.ehcache.internal.TestTimeSource;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Henri Tremblay
 */
public class OnHeapStrategyTest {

  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  @Mock
  private OnHeapStore<Integer, String> store;

  @Mock
  private ExpiryPolicy<Integer, String> policy;

  private OnHeapStrategy<Integer, String> strategy;

  private static class TestOnHeapValueHolder extends OnHeapValueHolder<String> {

    long now;
    Duration expiration;

    protected TestOnHeapValueHolder(long expirationTime) {
      super(1, 0, expirationTime, true);
    }

    @Override
    public String get() {
      return "test";
    }

    @Override
    public void accessed(long now, Duration expiration) {
      this.now = now;
      this.expiration = expiration;
      super.accessed(now, expiration);
    }
  }

  private TestTimeSource timeSource = new TestTimeSource();

  @Test
  public void isExpired_10seconds() {
    strategy = OnHeapStrategy.strategy(store, policy, timeSource);

    TestOnHeapValueHolder mapping = new TestOnHeapValueHolder(10);
    assertThat(strategy.isExpired(mapping)).isFalse();
    timeSource.advanceTime(10);
    assertThat(strategy.isExpired(mapping)).isTrue();
  }

  @Test
  public void isExpired_TTL10seconds() {
    strategy = OnHeapStrategy.strategy(store, policy, timeSource);

    TestOnHeapValueHolder mapping = new TestOnHeapValueHolder(10);
    assertThat(strategy.isExpired(mapping)).isFalse();
    timeSource.advanceTime(10);
    assertThat(strategy.isExpired(mapping)).isTrue();
  }

  @Test
  public void isExpired_neverExpires() {
    strategy = OnHeapStrategy.strategy(store, ExpiryPolicy.NO_EXPIRY, timeSource);

    TestOnHeapValueHolder mapping = new TestOnHeapValueHolder(10);
    assertThat(strategy.isExpired(mapping)).isFalse();
    timeSource.advanceTime(10);
    assertThat(strategy.isExpired(mapping)).isFalse();
  }

  @Test
  public void setAccessTimeAndExpiryThenReturnMappingOutsideLock_nullExpiryForAccess() {
    strategy = OnHeapStrategy.strategy(store, ExpiryPolicy.NO_EXPIRY, timeSource);

    TestOnHeapValueHolder mapping = new TestOnHeapValueHolder(10);
    when(policy.getExpiryForAccess(1, mapping)).thenReturn(null);

    strategy.setAccessAndExpiryTimeWhenCallerOutsideLock(1, mapping, timeSource.getTimeMillis());

    assertThat(mapping.expiration).isNull();
    assertThat(mapping.now).isEqualTo(timeSource.getTimeMillis());

    verifyZeroInteractions(store);
  }

  @Test
  public void setAccessTimeAndExpiryThenReturnMappingOutsideLock_zeroExpiryOnAccess() {
    strategy = OnHeapStrategy.strategy(store, policy, timeSource);

    TestOnHeapValueHolder mapping = new TestOnHeapValueHolder(10);
    when(policy.getExpiryForAccess(1, mapping)).thenReturn(Duration.ZERO);

    strategy.setAccessAndExpiryTimeWhenCallerOutsideLock(1, mapping, timeSource.getTimeMillis());

    verify(store).expireMappingUnderLock(1, mapping);
  }

  @Test
  public void setAccessTimeAndExpiryThenReturnMappingOutsideLock_infiniteExpiryOnAccess() {
    strategy = OnHeapStrategy.strategy(store, policy, timeSource);

    TestOnHeapValueHolder mapping = new TestOnHeapValueHolder(10);
    when(policy.getExpiryForAccess(1, mapping)).thenReturn(ExpiryPolicy.INFINITE);

    strategy.setAccessAndExpiryTimeWhenCallerOutsideLock(1, mapping, timeSource.getTimeMillis());

    assertThat(mapping.expiration).isEqualTo(ExpiryPolicy.INFINITE);
    assertThat(mapping.now).isEqualTo(timeSource.getTimeMillis());

    verifyZeroInteractions(store);
  }

  @Test
  public void setAccessTimeAndExpiryThenReturnMappingOutsideLock_movingTime() {
    strategy = OnHeapStrategy.strategy(store, policy, timeSource);

    TestOnHeapValueHolder mapping = new TestOnHeapValueHolder(10);
    when(policy.getExpiryForAccess(1, mapping)).thenReturn(Duration.ofMillis(20));

    strategy.setAccessAndExpiryTimeWhenCallerOutsideLock(1, mapping, timeSource.getTimeMillis());

    assertThat(mapping.expiration).isEqualTo(Duration.ofMillis(20));
    assertThat(mapping.now).isEqualTo(timeSource.getTimeMillis());

    verifyZeroInteractions(store);

    timeSource.advanceTime(30);

    strategy.setAccessAndExpiryTimeWhenCallerOutsideLock(1, mapping, timeSource.getTimeMillis());

    assertThat(mapping.expiration).isEqualTo(Duration.ofMillis(20));
    assertThat(mapping.now).isEqualTo(timeSource.getTimeMillis());

    verifyZeroInteractions(store);
  }
}
