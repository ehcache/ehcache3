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
package org.ehcache.internal.store.heap.bytesized;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;

import java.util.concurrent.TimeUnit;

import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.internal.copy.IdentityCopier;
import org.ehcache.internal.serialization.CompactJavaSerializer;
import org.ehcache.internal.sizeof.DefaultSizeOfEngine;
import org.ehcache.internal.store.heap.OnHeapStore;
import org.ehcache.internal.store.heap.holders.CopiedOnHeapValueHolder;
import org.ehcache.internal.store.heap.holders.OnHeapValueHolder;
import org.ehcache.internal.store.heap.holders.SerializedOnHeapValueHolder;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.sizeof.SizeOfEngine;
import org.junit.Test;
import org.mockito.Matchers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Abhilash
 *
 */
public class ByteAccountingTest {
  
  private static final Copier DEFAULT_COPIER = new IdentityCopier();
  private static final SizeOfEngine SIZE_OF_ENGINE = new DefaultSizeOfEngine(Long.MAX_VALUE, Long.MAX_VALUE);
  
  private static final String KEY = "key";
  private static final String  VALUE = "value";
  private static final long SIZE_OF_KEY_VALUE_PAIR = getSize(KEY, VALUE);


  <K, V> OnHeapStoreForTests<K, V> newStore() {
    return newStore(SystemTimeSource.INSTANCE, Expirations.noExpiration(), Eviction.none());
  }
  
  <K, V> OnHeapStoreForTests<K, V> newStore(int capacity) {
    return newStore(SystemTimeSource.INSTANCE, Expirations.noExpiration(), Eviction.none(), capacity);
  }
  
  <K, V> OnHeapStoreForTests<K, V> newStore(TimeSource timeSource, Expiry<? super K, ? super V> expiry) {
    return newStore(timeSource, expiry, Eviction.none());
  }
  
  <K, V> OnHeapStoreForTests<K, V> newStore(TimeSource timeSource, Expiry<? super K, ? super V> expiry, EvictionVeto<? super K, ? super V> veto) {
    return newStore(timeSource, expiry, veto, 100);
  }
  
  private <K, V> OnHeapStoreForTests<K, V> newStore(final TimeSource timeSource, final Expiry<? super K, ? super V> expiry, final EvictionVeto<? super K, ? super V> veto,
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
      public EvictionVeto<? super K, ? super V> getEvictionVeto() {
        return veto;
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
    }, timeSource, new DefaultSizeOfEngine(Long.MAX_VALUE, Long.MAX_VALUE));
  }
  
  @Test
  public void testPut() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
  }

  @Test
  public void testRemove() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    long beforeRemove = store.getCurrentUsageInBytes();
    store.remove("Another Key");
    assertThat(store.getCurrentUsageInBytes(), is(beforeRemove));
    store.remove(KEY);
    assertThat(store.getCurrentUsageInBytes(), is(0l));
  }

  @Test
  public void testRemoveTwoArg() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    long beforeRemove = store.getCurrentUsageInBytes();
    store.remove(KEY, "Another value");
    assertThat(store.getCurrentUsageInBytes(), is(beforeRemove));
    store.remove(KEY, VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(0l));
  }
  
  @Test
  public void testReplace() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    long beforeReplace = store.getCurrentUsageInBytes();
    store.replace("Another Key", "Another Value");
    assertThat(store.getCurrentUsageInBytes(), is(beforeReplace));
    
    String toReplace = "Replaced Value";
    store.replace(KEY, toReplace);
    long delta = SIZE_OF_ENGINE.sizeof(toReplace) - SIZE_OF_ENGINE.sizeof(VALUE);
    long afterReplace = store.getCurrentUsageInBytes();
    
    assertThat(afterReplace - beforeReplace, is(delta));
    
    //when delta is negative
    store.replace(KEY, VALUE);
    assertThat(afterReplace - store.getCurrentUsageInBytes(), is(delta));
  }
  
  @Test
  public void testReplaceThreeArg() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    long beforeReplace = store.getCurrentUsageInBytes();
    store.replace(KEY, "Another Value", VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(beforeReplace));
    
    String toReplace = "Replaced Value";
    store.replace(KEY, VALUE, toReplace);
    long delta = SIZE_OF_ENGINE.sizeof(toReplace) - SIZE_OF_ENGINE.sizeof(VALUE);
    long afterReplace = store.getCurrentUsageInBytes();
    
    assertThat(afterReplace - beforeReplace, is(delta));
    
    //when delta is negative
    store.replace(KEY, toReplace, VALUE);
    assertThat(afterReplace - store.getCurrentUsageInBytes(), is(delta));
  }
  
  @Test
  public void testPutIfAbsent() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.putIfAbsent(KEY, VALUE);
    long curent = 0l;
    assertThat(curent = store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    
    store.putIfAbsent(KEY, "New Value to Put");
    assertThat(store.getCurrentUsageInBytes(), is(curent));
  }
  
  @Test
  public void testComputeRemove() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();
    
    store.put(KEY, VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    
    store.compute("another", new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return null;
      }
    });
    
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    
    store.compute(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return null;
      }
    });
    
    assertThat(store.getCurrentUsageInBytes(), is(0l));
  }
  
  @Test
  public void testCompute() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.compute(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return VALUE;
      }
    });
    
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    
    final String replace = "Replace the original value";
    long delta = SIZE_OF_ENGINE.sizeof(replace) - SIZE_OF_ENGINE.sizeof(VALUE);
    
    store.compute(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return replace;
      }
    });
    
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR + delta));
  }
  
  @Test
  public void testComputeIfAbsent() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.computeIfAbsent(KEY, new Function<String, String>() {

      @Override
      public String apply(String a) {
        return VALUE;
      }
    });
    
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    
    store.computeIfAbsent(KEY, new Function<String, String>() {

      @Override
      public String apply(String a) {
        return "Should not be replaced";
      }
    });
    
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    
  }
  
  @Test
  public void testComputeIfPresentRemove() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    
    store.computeIfPresent("another", new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return null;
      }
    });
    
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    
    store.computeIfPresent(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return null;
      }
    });
    
    assertThat(store.getCurrentUsageInBytes(), is(0l));
    
  }
  
  @Test
  public void testComputeIfPresent() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();
    
    store.computeIfPresent(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return VALUE;
      }
    });
    
    assertThat(store.getCurrentUsageInBytes(), is(0l));
    
    store.put(KEY, VALUE);
    
    store.computeIfPresent(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return VALUE;
      }
    });
    
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    
    final String replace = "Replace the original value";
    long delta = SIZE_OF_ENGINE.sizeof(replace) - SIZE_OF_ENGINE.sizeof(VALUE);
    
    store.compute(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return replace;
      }
    });
    
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR + delta));
    
  }
  
  @Test
  public void testExpiry() throws CacheAccessException {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStoreForTests<String, String> store = newStore(timeSource, Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));

    store.put(KEY, VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    timeSource.advanceTime(1);
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    assertThat(store.get(KEY), nullValue());
    assertThat(store.getCurrentUsageInBytes(), is(0l));
  }
  
  @Test
  public void testEviction() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore(1);
    StoreEventListener<String, String> listener = mock(StoreEventListener.class);
    store.enableStoreEventNotifications(listener);
    
    store.put(KEY, VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    
    String key1 = "key1";
    String value1 = new String(new byte[200]);
    
    long requiredSize = getSize(key1, value1);
    
    store.put(key1, value1);
    verify(listener, times(1)).onEviction(Matchers.<String>any(), Matchers.<Store.ValueHolder<String>>any());
    if (store.get(key1) != null) {
      assertThat(store.getCurrentUsageInBytes(), is(requiredSize));
    } else {
      assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    }
    
  }
  
  static long getSize(String key, String value) {
    CopiedOnHeapValueHolder<String> valueHolder = new CopiedOnHeapValueHolder<String>(value, 0l, 0l, true, DEFAULT_COPIER);
    return SIZE_OF_ENGINE.sizeof(ConcurrentHashMap.FAKE_TREE_BIN) + SIZE_OF_ENGINE.sizeofKey(key)
        + SIZE_OF_ENGINE.sizeof(valueHolder);
  } 
  
  public static class OnHeapStoreForTests<K, V> extends OnHeapStore<K, V> {

    private static final Copier DEFAULT_COPIER = new IdentityCopier();

    public OnHeapStoreForTests(final Configuration<K, V> config, final TimeSource timeSource, final SizeOfEngine engine) {
      super(config, timeSource, DEFAULT_COPIER, DEFAULT_COPIER, engine);
    }
    
    @Override
    public long getCurrentUsageInBytes() {
      return super.getCurrentUsageInBytes();
    }

  }
}
