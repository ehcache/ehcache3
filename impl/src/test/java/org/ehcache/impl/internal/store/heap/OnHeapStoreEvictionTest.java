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

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.event.EventType;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.impl.internal.store.heap.holders.OnHeapValueHolder;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.store.Store;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.core.spi.store.heap.SizeOfEngine;
import org.junit.Test;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import static org.ehcache.config.Eviction.noAdvice;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class OnHeapStoreEvictionTest {

  protected <K, V> OnHeapStoreForTests<K, V> newStore() {
    return newStore(SystemTimeSource.INSTANCE, null);
  }

  /** eviction tests : asserting the evict method is called **/

  @Test
  public void testComputeCalledEnforceCapacity() throws Exception {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put("key", "value");
    store.getAndCompute("key", (mappedKey, mappedValue) -> "value2");

    assertThat(store.enforceCapacityWasCalled(), is(true));
  }

  @Test
  public void testComputeIfAbsentCalledEnforceCapacity() throws Exception {
    OnHeapStoreForTests<String, String> store = newStore();

    store.computeIfAbsent("key", mappedKey -> "value2");

    assertThat(store.enforceCapacityWasCalled(), is(true));
  }

  @Test
  public void testFaultsDoNotGetToEvictionAdvisor() throws StoreAccessException {
    final Semaphore semaphore = new Semaphore(0);

    final OnHeapStoreForTests<String, String> store = newStore(SystemTimeSource.INSTANCE, noAdvice());

    ExecutorService executor = Executors.newCachedThreadPool();
    try {
      executor.submit(() -> store.getOrComputeIfAbsent("prime", key -> {
        semaphore.acquireUninterruptibly();
        return new OnHeapValueHolder<String>(0, 0, false) {
          @Override
          public String get() {
            return key;
          }
        };
      }));

      while (!semaphore.hasQueuedThreads());
      store.put("boom", "boom");
    } finally {
      semaphore.release(1);
      executor.shutdown();
    }
  }

  @Test
  public void testEvictionCandidateLimits() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    StoreConfigurationImpl<String, String> configuration = new StoreConfigurationImpl<>(
      String.class, String.class, noAdvice(),
      getClass().getClassLoader(), ExpiryPolicyBuilder.noExpiration(), heap(1).build(), 1, null, null);
    TestStoreEventDispatcher<String, String> eventDispatcher = new TestStoreEventDispatcher<>();
    final String firstKey = "daFirst";
    eventDispatcher.addEventListener(event -> {
      if (event.getType().equals(EventType.EVICTED)) {
        assertThat(event.getKey(), is(firstKey));
      }
    });
    OnHeapStore<String, String> store = new OnHeapStore<>(configuration, timeSource,
      new IdentityCopier<>(), new IdentityCopier<>(), new NoopSizeOfEngine(), eventDispatcher, new DefaultStatisticsService());
    timeSource.advanceTime(10000L);
    store.put(firstKey, "daValue");
    timeSource.advanceTime(10000L);
    store.put("other", "otherValue");
  }

  protected <K, V> OnHeapStoreForTests<K, V> newStore(final TimeSource timeSource,
      final EvictionAdvisor<? super K, ? super V> evictionAdvisor) {
    return new OnHeapStoreForTests<>(new Store.Configuration<K, V>() {
      @SuppressWarnings("unchecked")
      @Override
      public Class<K> getKeyType() {
        return (Class<K>) String.class;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Class<V> getValueType() {
        return (Class<V>) Serializable.class;
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
        return ExpiryPolicyBuilder.noExpiration();
      }

      @Override
      public ResourcePools getResourcePools() {
        return newResourcePoolsBuilder().heap(1, EntryUnit.ENTRIES).build();
      }

      @Override
      public Serializer<K> getKeySerializer() {
        throw new AssertionError();
      }

      @Override
      public Serializer<V> getValueSerializer() {
        throw new AssertionError();
      }

      @Override
      public int getDispatcherConcurrency() {
        return 1;
      }

      @Override
      public CacheLoaderWriter<? super K, V> getCacheLoaderWriter() {
        return null;
      }
    }, timeSource);
  }

  public static class OnHeapStoreForTests<K, V> extends OnHeapStore<K, V> {

    public OnHeapStoreForTests(final Configuration<K, V> config, final TimeSource timeSource) {
      super(config, timeSource, IdentityCopier.identityCopier(), IdentityCopier.identityCopier(),  new NoopSizeOfEngine(),
        NullStoreEventDispatcher.nullStoreEventDispatcher(), new DefaultStatisticsService());
    }

    public OnHeapStoreForTests(final Configuration<K, V> config, final TimeSource timeSource, final SizeOfEngine engine) {
      super(config, timeSource, IdentityCopier.identityCopier(), IdentityCopier.identityCopier(), engine,
        NullStoreEventDispatcher.nullStoreEventDispatcher(), new DefaultStatisticsService());
    }

    private boolean enforceCapacityWasCalled = false;

    @Override
    protected void enforceCapacity() {
      enforceCapacityWasCalled = true;
      super.enforceCapacity();
    }

    boolean enforceCapacityWasCalled() {
      return enforceCapacityWasCalled;
    }

  }

}
