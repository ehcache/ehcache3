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
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.DefaultSizeOfEngine;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.heap.bytesized.ByteAccountingTest.OnHeapStoreForTests;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
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
    return newStore(SystemTimeSource.INSTANCE, ExpiryPolicyBuilder.noExpiration(), Eviction.noAdvice(), 100);
  }

  private <K, V> OnHeapStoreForTests<K, V> newStore(final TimeSource timeSource, final ExpiryPolicy<? super K, ? super V> expiry, final EvictionAdvisor<? super K, ? super V> evictionAdvisor,
                                                    final int capacity) {

    return new OnHeapStoreForTests<>(new Store.Configuration<K, V>() {
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
      public ExpiryPolicy<? super K, ? super V> getExpiry() {
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

      @Override
      public CacheLoaderWriter<? super K, V> getCacheLoaderWriter() {
        return null;
      }
    }, timeSource, new DefaultSizeOfEngine(Long.MAX_VALUE, 1000), new TestStoreEventDispatcher<>());
  }

  private static void assertNullMapping(OnHeapStore<String, String> store) throws Exception {
    assertThat(store.get(KEY), nullValue());
  }

  private static void assertNotNullMapping(OnHeapStore<String, String> store) throws Exception {
    assertThat(store.get(KEY).get(), equalTo(VALUE));
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

    store.putIfAbsent(KEY, OVER_SIZED_VALUE, b -> {});
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

    store.getAndCompute(KEY, (a, b) -> OVER_SIZED_VALUE);

    assertNullMapping(store);

    store.getAndCompute(KEY, (a, b) -> VALUE);

    assertNotNullMapping(store);

    store.getAndCompute(KEY, (a, b) -> OVER_SIZED_VALUE);

    assertNullMapping(store);
  }

  @Test
  public void testComputeIfAbsent() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.computeIfAbsent(KEY, a -> OVER_SIZED_VALUE);
    assertNullMapping(store);

    store.put(KEY, VALUE);
    store.computeIfAbsent(KEY, a -> OVER_SIZED_VALUE);

    assertNotNullMapping(store);
  }

}
