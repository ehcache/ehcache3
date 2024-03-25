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
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.store.shared.authoritative.AuthoritativeTierPartition;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.junit.After;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assume.assumeThat;

@SuppressWarnings({"unchecked", "rawtypes"})
public class SharedOffHeapAsAuthoritativeTierTest {
  private final TestTimeSource timeSource = new TestTimeSource();
  private SharedOffHeapStoreTest.SharedOffHeapStore sharedStore;
  private AuthoritativeTierPartition<String, String> authoritativeTierPartition;

  @After
  public void after() {
    if (sharedStore != null) {
      sharedStore.destroy(authoritativeTierPartition);
    }
  }

  private void createAndInitStore(TimeSource timeSource, ExpiryPolicy<? super String, ? super String> expiry) {
    try {
      sharedStore = new SharedOffHeapStoreTest.SharedOffHeapStore(timeSource);
      authoritativeTierPartition = sharedStore.createAuthoritativeTierPartition(1, String.class, String.class, Eviction.noAdvice(), expiry);
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void testFlushUpdatesAccessStats() throws StoreAccessException {
    ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L));
    createAndInitStore(timeSource, expiry);
    final String key = "foo";
    final String value = "bar";
    authoritativeTierPartition.put(key, value);
    final SimpleValueHolder<String> firstValueHolder = new SimpleValueHolder<>(authoritativeTierPartition.getAndFault(key));
    authoritativeTierPartition.put(key, value);
    final SimpleValueHolder<String> secondValueHolder = new SimpleValueHolder<>(authoritativeTierPartition.getAndFault(key));
    timeSource.advanceTime(10);
    firstValueHolder.accessed(timeSource.getTimeMillis(), expiry.getExpiryForAccess(key, () -> value));
    timeSource.advanceTime(10);
    secondValueHolder.accessed(timeSource.getTimeMillis(), expiry.getExpiryForAccess(key, () -> value));
    assertThat(authoritativeTierPartition.flush(key, firstValueHolder), is(false));
    assertThat(authoritativeTierPartition.flush(key, secondValueHolder), is(true));
    timeSource.advanceTime(10); // this should NOT affect
    assertThat(authoritativeTierPartition.getAndFault(key).lastAccessTime(), is(secondValueHolder.creationTime() + 20));
  }

  @Test
  public void testGetAndFaultOnExpiredEntry() throws StoreAccessException {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(10L)));
    authoritativeTierPartition.put("key", "value");
    timeSource.advanceTime(20L);
    Store.ValueHolder<String> valueHolder = authoritativeTierPartition.getAndFault("key");
    assertThat(valueHolder, nullValue());
    assertThat(SharedOffHeapStoreTest.getExpirationStatistic(sharedStore.getSharedStore()).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
    assumeThat(SharedOffHeapStoreTest.getExpirationStatistic(authoritativeTierPartition).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
  }

  static class SimpleValueHolder<T> extends AbstractValueHolder<T> {

    final private T value;

    public SimpleValueHolder(Store.ValueHolder<T> valueHolder) {
      super(valueHolder.getId(), valueHolder.creationTime(), valueHolder.expirationTime());
      this.value = valueHolder.get();
    }

    @Nonnull
    @Override
    public T get() {
      return value;
    }
  }
}
