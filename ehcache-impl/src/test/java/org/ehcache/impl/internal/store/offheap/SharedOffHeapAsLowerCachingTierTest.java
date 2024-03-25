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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.config.Eviction;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.core.spi.store.AbstractValueHolder;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.statistics.LowerCachingTierOperationsOutcome;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.store.shared.caching.lower.LowerCachingTierPartition;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.junit.After;
import org.junit.Test;

import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.impl.internal.util.StatisticsTestUtils.validateStats;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@SuppressWarnings({"unchecked", "rawtypes"})
public class SharedOffHeapAsLowerCachingTierTest {
  private final TestTimeSource timeSource = new TestTimeSource();
  private SharedOffHeapStoreTest.SharedOffHeapStore sharedStore;
  private LowerCachingTierPartition<String, String> lowerCachingTierPartition;

  @After
  public void after() {
    if (sharedStore != null) {
      sharedStore.destroy(lowerCachingTierPartition);
    }
  }

  private void createAndInitStore(TimeSource timeSource, ExpiryPolicy<? super String, ? super String> expiry) {
    try {
      sharedStore = new SharedOffHeapStoreTest.SharedOffHeapStore(timeSource);
      lowerCachingTierPartition = sharedStore.createLowerCachingTierPartition(1, String.class, String.class, Eviction.noAdvice(), expiry);
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void testGetAndRemoveExpiredElementReturnsNull() throws Exception {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));
    assertThat(lowerCachingTierPartition.getAndRemove("1"), is(nullValue()));
    lowerCachingTierPartition.installMapping("1", key -> new AbstractOffHeapStoreTest.SimpleValueHolder<>("one", timeSource.getTimeMillis(), 15));
    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<>();
    lowerCachingTierPartition.setInvalidationListener((key, valueHolder) -> {
      valueHolder.get();
      invalidated.set(valueHolder);
    });
    timeSource.advanceTime(20);
    assertThat(lowerCachingTierPartition.getAndRemove("1"), is(nullValue()));
    assertThat(invalidated.get().get(), equalTo("one"));
    assertThat(invalidated.get().isExpired(timeSource.getTimeMillis()), is(true));
    assertThat(SharedOffHeapStoreTest.getExpirationStatistic(sharedStore.getSharedStore()).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
    //assumeThat(getExpirationStatistic((Store<?, ?>) lowerCachingTierPartition).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
  }

  @Test
  public void testInstallMapping() throws Exception {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));
    assertThat(lowerCachingTierPartition.installMapping("1", key -> new SimpleValueHolder<>("one", timeSource.getTimeMillis(), 15)).get(), equalTo("one"));
    validateStats(sharedStore.getSharedStore(), EnumSet.of(LowerCachingTierOperationsOutcome.InstallMappingOutcome.PUT));
    //validateStats(lowerCachingTierPartition, EnumSet.of(LowerCachingTierOperationsOutcome.InstallMappingOutcome.PUT));
    timeSource.advanceTime(20);
    try {
      lowerCachingTierPartition.installMapping("1", key -> new SimpleValueHolder<>("un", timeSource.getTimeMillis(), 15));
      fail("expected AssertionError");
    } catch (AssertionError ae) {
      // expected
    }
  }

  @Test
  public void testInvalidateKeyAbsent() throws Exception {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));
    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<>();
    lowerCachingTierPartition.setInvalidationListener((key, valueHolder) -> invalidated.set(valueHolder));
    lowerCachingTierPartition.invalidate("1");
    assertThat(invalidated.get(), is(nullValue()));
    validateStats(sharedStore.getSharedStore(), EnumSet.of(LowerCachingTierOperationsOutcome.InvalidateOutcome.MISS));
    //validateStats(lowerCachingTierPartition, EnumSet.of(LowerCachingTierOperationsOutcome.InvalidateOutcome.MISS));
  }

  @Test
  public void testInvalidateKeyPresent() throws Exception {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));
    assertThat(lowerCachingTierPartition.installMapping("1", key -> new SimpleValueHolder<>("one", timeSource.getTimeMillis(), 15)).get(), equalTo("one"));
    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<>();
    lowerCachingTierPartition.setInvalidationListener((key, valueHolder) -> {
      valueHolder.get();
      invalidated.set(valueHolder);
    });
    lowerCachingTierPartition.invalidate("1");
    assertThat(invalidated.get().get(), equalTo("one"));
    validateStats(sharedStore.getSharedStore(), EnumSet.of(LowerCachingTierOperationsOutcome.InvalidateOutcome.REMOVED));
    //validateStats(lowerCachingTierPartition, EnumSet.of(LowerCachingTierOperationsOutcome.InvalidateOutcome.REMOVED));
    assertThat(lowerCachingTierPartition.get("1"), is(nullValue()));
  }

  static class SimpleValueHolder<T> extends AbstractValueHolder<T> {

    private final T value;

    public SimpleValueHolder(T v, long creationTime, long expirationTime) {
      super(-1, creationTime, expirationTime);
      this.value = v;
    }

    @Override
    public T get() {
      return value;
    }

    @Override
    public long creationTime() {
      return 0;
    }

    @Override
    public long expirationTime() {
      return 0;
    }

    @Override
    public boolean isExpired(long expirationTime) {
      return false;
    }

    @Override
    public long lastAccessTime() {
      return 0;
    }

    @Override
    public long getId() {
      return 0;
    }
  }
}
