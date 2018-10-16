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

import org.ehcache.Cache;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.copy.Copier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.test.MockitoUtil.mock;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * OnHeapStoreValueCopierTest
 */
@RunWith(Parameterized.class)
public class OnHeapStoreValueCopierTest {

  private static final Long KEY = 42L;
  public static final Value VALUE = new Value("TheAnswer");
  public static final Supplier<Boolean> NOT_REPLACE_EQUAL = () -> false;
  public static final Supplier<Boolean> REPLACE_EQUAL = () -> true;

  @Parameterized.Parameters(name = "copyForRead: {0} - copyForWrite: {1}")
  public static Collection<Object[]> config() {
    return Arrays.asList(new Object[][] {
        {false, false}, {false, true}, {true, false}, {true, true}
    });
  }

  @Parameterized.Parameter(value = 0)
  public boolean copyForRead;

  @Parameterized.Parameter(value = 1)
  public boolean copyForWrite;

  private OnHeapStore<Long, Value> store;

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Before
  public void setUp() {
    Store.Configuration<Long, Value> configuration = mock(Store.Configuration.class);
    when(configuration.getResourcePools()).thenReturn(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build());
    when(configuration.getKeyType()).thenReturn(Long.class);
    when(configuration.getValueType()).thenReturn(Value.class);

    ExpiryPolicy expiryPolicy = ExpiryPolicyBuilder.noExpiration();
    when(configuration.getExpiry()).thenReturn(expiryPolicy);

    Copier<Value> valueCopier = new Copier<Value>() {
      @Override
      public Value copyForRead(Value obj) {
        if (copyForRead) {
          return new Value(obj.state);
        }
        return obj;
      }

      @Override
      public Value copyForWrite(Value obj) {
        if (copyForWrite) {
          return new Value(obj.state);
        }
        return obj;
      }
    };

    store = new OnHeapStore<>(configuration, SystemTimeSource.INSTANCE, new IdentityCopier<>(), valueCopier, new NoopSizeOfEngine(), NullStoreEventDispatcher.<Long, Value>nullStoreEventDispatcher());
  }

  @Test
  public void testPutAndGet() throws StoreAccessException {
    store.put(KEY, VALUE);

    Store.ValueHolder<Value> firstStoreValue = store.get(KEY);
    Store.ValueHolder<Value> secondStoreValue = store.get(KEY);
    compareValues(VALUE, firstStoreValue.get());
    compareValues(VALUE, secondStoreValue.get());
    compareReadValues(firstStoreValue.get(), secondStoreValue.get());
  }

  @Test
  public void testGetAndCompute() throws StoreAccessException {
    store.put(KEY, VALUE);
    Store.ValueHolder<Value> computedVal = store.getAndCompute(KEY, (aLong, value) -> VALUE);
    Store.ValueHolder<Value> oldValue = store.get(KEY);
    store.getAndCompute(KEY, (aLong, value) -> {
      compareReadValues(value, oldValue.get());
      return value;
    });

    compareValues(VALUE, computedVal.get());
  }

  @Test
  public void testComputeWithoutReplaceEqual() throws StoreAccessException {
    final Store.ValueHolder<Value> firstValue = store.computeAndGet(KEY, (aLong, value) -> VALUE, NOT_REPLACE_EQUAL, () -> false);
    store.computeAndGet(KEY, (aLong, value) -> {
      compareReadValues(value, firstValue.get());
      return value;
    }, NOT_REPLACE_EQUAL, () -> false);

    compareValues(VALUE, firstValue.get());
  }

  @Test
  public void testComputeWithReplaceEqual() throws StoreAccessException {
    final Store.ValueHolder<Value> firstValue = store.computeAndGet(KEY, (aLong, value) -> VALUE, REPLACE_EQUAL, () -> false);
    store.computeAndGet(KEY, (aLong, value) -> {
      compareReadValues(value, firstValue.get());
      return value;
    }, REPLACE_EQUAL, () -> false);

    compareValues(VALUE, firstValue.get());
  }

  @Test
  public void testComputeIfAbsent() throws StoreAccessException {
    Store.ValueHolder<Value> computedValue = store.computeIfAbsent(KEY, aLong -> VALUE);
    Store.ValueHolder<Value> secondComputedValue = store.computeIfAbsent(KEY, aLong -> {
      fail("There should have been a mapping");
      return null;
    });
    compareValues(VALUE, computedValue.get());
    compareReadValues(computedValue.get(), secondComputedValue.get());
  }

  @Test
  public void testBulkCompute() throws StoreAccessException {
    final Map<Long, Store.ValueHolder<Value>> results = store.bulkCompute(singleton(KEY), entries -> singletonMap(KEY, VALUE).entrySet());
    store.bulkCompute(singleton(KEY), entries -> {
      compareReadValues(results.get(KEY).get(), entries.iterator().next().getValue());
      return entries;
    });
    compareValues(VALUE, results.get(KEY).get());
  }

  @Test
  public void testBulkComputeWithoutReplaceEqual() throws StoreAccessException {
    final Map<Long, Store.ValueHolder<Value>> results = store.bulkCompute(singleton(KEY), entries -> singletonMap(KEY, VALUE).entrySet(), NOT_REPLACE_EQUAL);
    store.bulkCompute(singleton(KEY), entries -> {
      compareReadValues(results.get(KEY).get(), entries.iterator().next().getValue());
      return entries;
    }, NOT_REPLACE_EQUAL);
    compareValues(VALUE, results.get(KEY).get());
  }

  @Test
  public void testBulkComputeWithReplaceEqual() throws StoreAccessException {
    final Map<Long, Store.ValueHolder<Value>> results = store.bulkCompute(singleton(KEY), entries -> singletonMap(KEY, VALUE).entrySet(), REPLACE_EQUAL);
    store.bulkCompute(singleton(KEY), entries -> {
      compareReadValues(results.get(KEY).get(), entries.iterator().next().getValue());
      return entries;
    }, REPLACE_EQUAL);
    compareValues(VALUE, results.get(KEY).get());
  }

  @Test
  public void testBulkComputeIfAbsent() throws StoreAccessException {
    Map<Long, Store.ValueHolder<Value>> results = store.bulkComputeIfAbsent(singleton(KEY), longs -> singletonMap(KEY, VALUE).entrySet());
    Map<Long, Store.ValueHolder<Value>> secondResults = store.bulkComputeIfAbsent(singleton(KEY), longs -> {
      fail("There should have been a mapping!");
      return null;
    });
    compareValues(VALUE, results.get(KEY).get());
    compareReadValues(results.get(KEY).get(), secondResults.get(KEY).get());
  }

  @Test
  public void testIterator() throws StoreAccessException {
    store.put(KEY, VALUE);
    Store.Iterator<Cache.Entry<Long, Store.ValueHolder<Value>>> iterator = store.iterator();
    assertThat(iterator.hasNext(), is(true));
    while (iterator.hasNext()) {
      Cache.Entry<Long, Store.ValueHolder<Value>> entry = iterator.next();
      compareValues(entry.getValue().get(), VALUE);
    }
  }

  private void compareValues(Value first, Value second) {
    if (copyForRead || copyForWrite) {
      assertThat(first, not(sameInstance(second)));
    } else {
      assertThat(first, sameInstance(second));
    }
  }

  private void compareReadValues(Value first, Value second) {
    if (copyForRead) {
      assertThat(first, not(sameInstance(second)));
    } else {
      assertThat(first, sameInstance(second));
    }
  }

  public static final class Value {
    String state;

    public Value(String state) {
      this.state = state;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Value value = (Value) o;
      return state.equals(value.state);
    }

    @Override
    public int hashCode() {
      return state.hashCode();
    }
  }
}
