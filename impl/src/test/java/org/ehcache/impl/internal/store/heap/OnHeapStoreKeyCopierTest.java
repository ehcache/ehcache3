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
import org.ehcache.core.statistics.DefaultStatisticsService;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * OnHeapStoreKeyCopierTest
 */
@RunWith(Parameterized.class)
public class OnHeapStoreKeyCopierTest {

  private static final Key KEY = new Key("WHat?");
  public static final String VALUE = "TheAnswer";
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

  private OnHeapStore<Key, String> store;

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Before
  public void setUp() {
    Store.Configuration<Key, String> configuration = mock(Store.Configuration.class);
    when(configuration.getResourcePools()).thenReturn(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build());
    when(configuration.getKeyType()).thenReturn(Key.class);
    when(configuration.getValueType()).thenReturn(String.class);
    when(configuration.getExpiry()).thenReturn((ExpiryPolicy) ExpiryPolicyBuilder.noExpiration());

    Copier<Key> keyCopier = new Copier<Key>() {
      @Override
      public Key copyForRead(Key obj) {
        if (copyForRead) {
          return new Key(obj);
        }
        return obj;
      }

      @Override
      public Key copyForWrite(Key obj) {
        if (copyForWrite) {
          return new Key(obj);
        }
        return obj;
      }
    };

    store = new OnHeapStore<>(configuration, SystemTimeSource.INSTANCE, keyCopier, IdentityCopier.identityCopier(),
      new NoopSizeOfEngine(), NullStoreEventDispatcher.nullStoreEventDispatcher(), new DefaultStatisticsService());
  }

  @Test
  public void testPutAndGet() throws StoreAccessException {
    Key copyKey = new Key(KEY);
    store.put(copyKey, VALUE);

    copyKey.state = "Different!";

    Store.ValueHolder<String> firstStoreValue = store.get(KEY);
    Store.ValueHolder<String> secondStoreValue = store.get(copyKey);
    if (copyForWrite) {
      assertThat(firstStoreValue.get(), is(VALUE));
      assertThat(secondStoreValue, nullValue());
    } else {
      assertThat(firstStoreValue, nullValue());
      assertThat(secondStoreValue.get(), is(VALUE));
    }
  }

  @Test
  public void testCompute() throws StoreAccessException {
    final Key copyKey = new Key(KEY);
    store.getAndCompute(copyKey, (key, value) -> {
      assertThat(key, is(copyKey));
      return VALUE;
    });
    copyKey.state = "Different!";
    store.getAndCompute(copyKey, (key, value) -> {
      if (copyForWrite) {
        assertThat(value, nullValue());
      } else {
        assertThat(value, is(VALUE));
        assertThat(key, is(copyKey));
        if (copyForRead) {
          key.state = "Changed!";
        }
      }
      return value;
    });

    if (copyForRead) {
      assertThat(copyKey.state, is("Different!"));
    }
  }

  @Test
  public void testComputeWithoutReplaceEqual() throws StoreAccessException {
    final Key copyKey = new Key(KEY);
    store.computeAndGet(copyKey, (key, value) -> {
      assertThat(key, is(copyKey));
      return VALUE;
    }, NOT_REPLACE_EQUAL, () -> false);
    copyKey.state = "Different!";
    store.computeAndGet(copyKey, (key, value) -> {
      if (copyForWrite) {
        assertThat(value, nullValue());
      } else {
        assertThat(value, is(VALUE));
        assertThat(key, is(copyKey));
        if (copyForRead) {
          key.state = "Changed!";
        }
      }
      return value;
    }, NOT_REPLACE_EQUAL, () -> false);

    if (copyForRead) {
      assertThat(copyKey.state, is("Different!"));
    }
  }

  @Test
  public void testComputeWithReplaceEqual() throws StoreAccessException {
    final Key copyKey = new Key(KEY);
    store.computeAndGet(copyKey, (key, value) -> {
      assertThat(key, is(copyKey));
      return VALUE;
    }, REPLACE_EQUAL, () -> false);
    copyKey.state = "Different!";
    store.computeAndGet(copyKey, (key, value) -> {
      if (copyForWrite) {
        assertThat(value, nullValue());
      } else {
        assertThat(value, is(VALUE));
        assertThat(key, is(copyKey));
        if (copyForRead) {
          key.state = "Changed!";
        }
      }
      return value;
    }, REPLACE_EQUAL, () -> false);

    if (copyForRead) {
      assertThat(copyKey.state, is("Different!"));
    }
  }

  @Test
  public void testIteration() throws StoreAccessException {
    store.put(KEY, VALUE);
    Store.Iterator<Cache.Entry<Key, Store.ValueHolder<String>>> iterator = store.iterator();
    assertThat(iterator.hasNext(), is(true));
    while (iterator.hasNext()) {
      Cache.Entry<Key, Store.ValueHolder<String>> entry = iterator.next();
      if (copyForRead || copyForWrite) {
        assertThat(entry.getKey(), not(sameInstance(KEY)));
      } else {
        assertThat(entry.getKey(), sameInstance(KEY));
      }
    }
  }

  @Test
  public void testComputeIfAbsent() throws StoreAccessException {
    store.computeIfAbsent(KEY, key -> {
      if (copyForRead || copyForWrite) {
        assertThat(key, not(sameInstance(KEY)));
      } else {
        assertThat(key, sameInstance(KEY));
      }
      return VALUE;
    });
  }

  @Test
  public void testBulkCompute() throws StoreAccessException {
    final AtomicReference<Key> keyRef = new AtomicReference<>();
    store.bulkCompute(singleton(KEY), entries -> {
      Key key = entries.iterator().next().getKey();
      if (copyForRead || copyForWrite) {
        assertThat(key, not(sameInstance(KEY)));
      } else {
        assertThat(key, sameInstance(KEY));
      }
      keyRef.set(key);
      return singletonMap(KEY, VALUE).entrySet();
    });

    store.bulkCompute(singleton(KEY), entries -> {
      if (copyForRead) {
        assertThat(entries.iterator().next().getKey(), not(sameInstance(keyRef.get())));
      }
      return singletonMap(KEY, VALUE).entrySet();
    });
  }

  @Test
  public void testBulkComputeWithoutReplaceEqual() throws StoreAccessException {
    store.bulkCompute(singleton(KEY), entries -> {
      if (copyForRead || copyForWrite) {
        assertThat(entries.iterator().next().getKey(), not(sameInstance(KEY)));
      } else {
        assertThat(entries.iterator().next().getKey(), sameInstance(KEY));
      }
      return singletonMap(KEY, VALUE).entrySet();
    }, NOT_REPLACE_EQUAL);
  }

  @Test
  public void testBulkComputeWithReplaceEqual() throws StoreAccessException {
    store.bulkCompute(singleton(KEY), entries -> {
      if (copyForRead || copyForWrite) {
        assertThat(entries.iterator().next().getKey(), not(sameInstance(KEY)));
      } else {
        assertThat(entries.iterator().next().getKey(), sameInstance(KEY));
      }
      return singletonMap(KEY, VALUE).entrySet();
    }, REPLACE_EQUAL);
  }

  @Test
  public void testBulkComputeIfAbsent() throws StoreAccessException {
    store.bulkComputeIfAbsent(singleton(KEY), keys -> {
      if (copyForWrite || copyForRead) {
        assertThat(keys.iterator().next(), not(sameInstance(KEY)));
      } else {
        assertThat(keys.iterator().next(), sameInstance(KEY));
      }
      return singletonMap(KEY, VALUE).entrySet();
    });
  }

  public static final class Key {
    String state;
    final int hashCode;

    public Key(String state) {
      this.state = state;
      this.hashCode = state.hashCode();
    }

    public Key(Key key) {
      this.state = key.state;
      this.hashCode = key.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Key value = (Key) o;
      return state.equals(value.state);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
