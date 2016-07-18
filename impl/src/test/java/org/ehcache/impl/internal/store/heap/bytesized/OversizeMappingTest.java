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

package org.ehcache.impl.internal.store.heap.bytesized;

import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.DefaultSizeOfEngine;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.heap.bytesized.ByteAccountingTest.OnHeapStoreForTests;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * @author Abhilash
 *
 */
public class OversizeMappingTest {

  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String OVER_SIZED_VALUE = new String(new byte[1000]);

  <K, V> OnHeapStoreForTests<K, V> newStore() {
    return newStore(SystemTimeSource.INSTANCE, Expirations.noExpiration(), Eviction.noAdvice(), 100);
  }

  private <K, V> OnHeapStoreForTests<K, V> newStore(final TimeSource timeSource, final Expiry<? super K, ? super V> expiry, final EvictionAdvisor<? super K, ? super V> evictionAdvisor,
      final int capacity) {

    return new OnHeapStoreForTests<K, V>(new Store.Configuration<K, V>() {
      @SuppressWarnings("unchecked")
      @Override
      public Class<K> getKeyType() {
        return (Class<K>) String.class;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Class<V> getValueType() {
        return (Class<V>) String.class;
      }

      @Override
      public EvictionAdvisor<? super K, ? super V> getEvictionAdvisor() {
        return evictionAdvisor;
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public Expiry<? super K, ? super V> getExpiry() {
        return expiry;
      }

      @Override
      public ResourcePools getResourcePools() {
        return newResourcePoolsBuilder().heap(capacity, MemoryUnit.KB).build();
      }

      @Override
      public Serializer<K> getKeySerializer() {
        throw new AssertionError("By-ref heap store using serializers!");
      }

      @Override
      public Serializer<V> getValueSerializer() {
        throw new AssertionError("By-ref heap store using serializers!");
      }

      @Override
      public int getDispatcherConcurrency() {
        return 0;
      }
    }, timeSource, new DefaultSizeOfEngine(Long.MAX_VALUE, 1000), new TestStoreEventDispatcher<K, V>());
  }

  private static void assertNullMapping(OnHeapStore<String, String> store) throws Exception {
    assertThat(store.get(KEY), nullValue());
  }

  private static void assertNotNullMapping(OnHeapStore<String, String> store) throws Exception {
    assertThat(store.get(KEY).value(), equalTo(VALUE));
  }

  @Test
  public void testPut() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put(KEY, OVER_SIZED_VALUE);
    assertNullMapping(store);
    store.put(KEY, VALUE);
    assertNotNullMapping(store);
  }

  @Test
  public void testPutIfAbsent() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.putIfAbsent(KEY, OVER_SIZED_VALUE);
    assertNullMapping(store);
  }

  @Test
  public void testReplace() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put(KEY, VALUE);
    store.replace(KEY, OVER_SIZED_VALUE);
    assertNullMapping(store);
  }

  @Test
  public void testThreeArgReplace() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put(KEY, VALUE);
    store.replace(KEY,VALUE, OVER_SIZED_VALUE);
    assertNullMapping(store);
  }

  @Test
  public void testCompute() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.compute(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return OVER_SIZED_VALUE;
      }
    });

    assertNullMapping(store);

    store.compute(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return VALUE;
      }
    });

    assertNotNullMapping(store);

    store.compute(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return OVER_SIZED_VALUE;
      }
    });

    assertNullMapping(store);
  }

  @Test
  public void testComputeIfAbsent() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.computeIfAbsent(KEY, new Function<String, String>() {

      @Override
      public String apply(String a) {
        return OVER_SIZED_VALUE;
      }
    });
    assertNullMapping(store);

    store.put(KEY, VALUE);
    store.computeIfAbsent(KEY, new Function<String, String>() {

      @Override
      public String apply(String a) {
        return OVER_SIZED_VALUE;
      }
    });

    assertNotNullMapping(store);
  }

}
