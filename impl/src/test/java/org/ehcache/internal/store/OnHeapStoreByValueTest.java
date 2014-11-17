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
package org.ehcache.internal.store;

import org.ehcache.Cache.Entry;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.SerializerException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.Predicate;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.Store.Iterator;
import org.ehcache.spi.cache.Store.ValueHolder;
import org.ehcache.spi.serialization.SerializationProvider;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

public class OnHeapStoreByValueTest {

  private static final RuntimeException RUNTIME_EXCEPTION = new RuntimeException();

  @Test
  public void testGet() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testGetExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    store.put("key", "value");
    assertThat(store.get("key").value(), equalTo("value"));
    timeSource.advanceTime(1);
    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testAccessTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, Expirations.noExpiration());
    store.put("key", "value");

    long first = store.get("key").lastAccessTime(TimeUnit.MILLISECONDS);
    assertThat(first, equalTo(timeSource.getTimeMillis()));
    final long advance = 5;
    timeSource.advanceTime(advance);
    long next = store.get("key").lastAccessTime(TimeUnit.MILLISECONDS);
    assertThat(next, equalTo(first + advance));
  }

  @Test
  public void testContainsKey() throws Exception {
    OnHeapStore<String, String> store = newStore();
    assertThat(store.containsKey("key"), is(false));
    store.put("key", "value");
    assertThat(store.containsKey("key"), is(true));
  }

  @Test
  public void testContainsKeyExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    assertThat(store.containsKey("key"), is(false));
    store.put("key", "value");
    assertThat(store.containsKey("key"), is(true));
    timeSource.advanceTime(1);
    assertThat(store.containsKey("key"), is(false));
  }

  @Test
  public void testPut() throws Exception {
    OnHeapStore<String, String> store = newStore();
    assertThat(store.get("key"), nullValue());
    store.put("key", "value");
    assertThat(store.get("key").value(), equalTo("value"));
    store.put("key", "value2");
    assertThat(store.get("key").value(), equalTo("value2"));
  }

  @Test
  public void testCreateTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, Expirations.noExpiration());
    assertThat(store.containsKey("key"), is(false));
    store.put("key", "value");
    ValueHolder<String> valueHolder = store.get("key");
    assertThat(timeSource.getTimeMillis(), equalTo(valueHolder.creationTime(TimeUnit.MILLISECONDS)));
  }

  @Test
  public void testRemove() throws Exception {
    OnHeapStore<String, String> store = newStore();
    assertThat(store.get("key"), nullValue());
    store.put("key", "value");
    assertThat(store.get("key").value(), equalTo("value"));
    store.remove("key");
    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testPutIfAbsent() throws Exception {
    OnHeapStore<String, String> store = newStore();
    assertThat(store.get("key"), nullValue());
    ValueHolder<String> prev = store.putIfAbsent("key", "value");
    assertThat(prev, nullValue());
    assertThat(store.get("key").value(), equalTo("value"));
    prev = store.putIfAbsent("key", "value2");
    assertThat(prev.value(), equalTo("value"));
    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testPutIfAbsentUpdatesAccessTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, Expirations.noExpiration());
    assertThat(store.get("key"), nullValue());
    store.putIfAbsent("key", "value");
    long first = store.get("key").lastAccessTime(TimeUnit.MILLISECONDS);
    timeSource.advanceTime(1);
    long next = store.putIfAbsent("key", "value2").lastAccessTime(TimeUnit.MILLISECONDS);
    assertThat(next - first, equalTo(1L));
  }

  @Test
  public void testPutIfAbsentExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    store.put("key", "value");
    timeSource.advanceTime(1);
    ValueHolder<String> prev = store.putIfAbsent("key", "value2");
    assertThat(prev, nullValue());
    assertThat(store.get("key").value(), equalTo("value2"));
  }

  @Test
  public void testRemoveTwoArg() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    assertThat(store.get("key").value(), equalTo("value"));
    boolean removed = store.remove("key", "not value");
    assertThat(removed, equalTo(false));
    assertThat(store.get("key").value(), equalTo("value"));
    removed = store.remove("key", "value");
    assertThat(removed, equalTo(true));
    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testRemoveTwoArgExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    store.put("key", "value");
    assertThat(store.get("key").value(), equalTo("value"));
    timeSource.advanceTime(1);
    boolean removed = store.remove("key", "value");
    assertThat(removed, equalTo(false));
  }

  @Test
  public void testReplaceTwoArg() throws Exception {
    OnHeapStore<String, String> store = newStore();

    assertThat(store.get("key"), nullValue());
    ValueHolder<String> existing = store.replace("key", "value");
    assertThat(existing, nullValue());
    assertThat(store.get("key"), nullValue());

    store.put("key", "value");
    existing = store.replace("key", "value2");
    assertThat(existing.value(), equalTo("value"));
    assertThat(store.get("key").value(), equalTo("value2"));
  }

  @Test
  public void testReplaceTwoArgExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));

    store.put("key", "value");
    timeSource.advanceTime(1);
    ValueHolder<String> existing = store.replace("key", "value2");
    assertThat(existing, nullValue());
    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testReplaceThreeArg() throws Exception {
    OnHeapStore<String, String> store = newStore();

    assertThat(store.get("key"), nullValue());
    boolean replaced = store.replace("key", "value", "value2");
    assertThat(replaced, equalTo(false));

    store.put("key", "value");

    replaced = store.replace("key", "not value", "value2");
    assertThat(replaced, equalTo(false));
    assertThat(store.get("key").value(), equalTo("value"));

    replaced = store.replace("key", "value", "value2");
    assertThat(replaced, equalTo(true));
    assertThat(store.get("key").value(), equalTo("value2"));
  }

  @Test
  public void testReplaceThreeArgExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));

    store.put("key", "value");
    timeSource.advanceTime(1);
    boolean replaced = store.replace("key", "value", "value2");
    assertThat(replaced, equalTo(false));
    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testIterator() throws Exception {
    OnHeapStore<String, String> store = newStore();

    Iterator<Entry<String, ValueHolder<String>>> iter = store.iterator();
    assertThat(iter.hasNext(), equalTo(false));
    try {
      iter.next();
      throw new AssertionError();
    } catch (NoSuchElementException nse) {
      // expected
    }

    store.put("key1", "value1");
    iter = store.iterator();
    assertThat(iter.hasNext(), equalTo(true));
    assertEntry(iter.next(), "key1", "value1");
    assertThat(iter.hasNext(), equalTo(false));

    store.put("key2", "value2");
    Map<String, String> observed = observe(store.iterator());
    assertThat(2, equalTo(observed.size()));
    assertThat(observed.get("key1"), equalTo("value1"));
    assertThat(observed.get("key2"), equalTo("value2"));
  }

  @Test
  public void testIteratorExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    store.put("key1", "value1");
    store.put("key2", "value2");
    timeSource.advanceTime(1);
    store.put("key3", "value3");

    Map<String, String> observed = observe(store.iterator());
    assertThat(1, equalTo(observed.size()));
    assertThat(observed.get("key3"), equalTo("value3"));

    timeSource.advanceTime(1);
    observed = observe(store.iterator());
    assertThat(0, equalTo(observed.size()));
  }

  @Test
  public void testIteratorUpdatesAccessTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, Expirations.noExpiration());

    store.put("key1", "value1");
    store.put("key2", "value2");

    timeSource.advanceTime(5);

    Map<String, Long> times = observeAccessTimes(store.iterator());
    assertThat(2, equalTo(times.size()));
    assertThat(times.get("key1"), equalTo(5L));
    assertThat(times.get("key2"), equalTo(5L));
  }

  @Test
  public void testCompute() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        assertThat(mappedKey, equalTo("key"));
        assertThat(mappedValue, nullValue());
        return "value";
      }
    });

    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testComputeNull() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        return null;
      }
    });
    assertThat(store.get("key"), nullValue());

    store.put("key", "value");
    assertThat(store.get("key").value(), equalTo("value"));
    store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        return null;
      }
    });
    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testComputeException() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    try {
      store.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String mappedKey, String mappedValue) {
          throw RUNTIME_EXCEPTION;
        }
      });
      throw new AssertionError();
    } catch (RuntimeException re) {
      assertThat(re, is(RUNTIME_EXCEPTION));
    }
    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testComputeExistingValue() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        assertThat(mappedKey, equalTo("key"));
        assertThat(mappedValue, equalTo("value"));
        return "value2";
      }
    });

    assertThat(store.get("key").value(), equalTo("value2"));
  }

  @Test
  public void testComputeExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    store.put("key", "value");
    timeSource.advanceTime(1);
    store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        assertThat(mappedKey, equalTo("key"));
        assertThat(mappedValue, nullValue());
        return "value2";
      }
    });

    assertThat(store.get("key").value(), equalTo("value2"));
  }

  @Test
  public void testComputeIfAbsent() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.computeIfAbsent("key", new Function<String, String>() {
      @Override
      public String apply(String mappedKey) {
        assertThat(mappedKey, equalTo("key"));
        return "value";
      }
    });

    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testComputeIfAbsentExisting() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    store.computeIfAbsent("key", new Function<String, String>() {
      @Override
      public String apply(String mappedKey) {
        throw new AssertionError();
      }
    });

    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testComputeIfAbsentReturnNull() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.computeIfAbsent("key", new Function<String, String>() {
      @Override
      public String apply(String mappedKey) {
        return null;
      }
    });

    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testComputeIfAbsentException() throws Exception {
    OnHeapStore<String, String> store = newStore();

    try {
      store.computeIfAbsent("key", new Function<String, String>() {
        @Override
        public String apply(String mappedKey) {
          throw RUNTIME_EXCEPTION;
        }
      });
      throw new AssertionError();
    } catch (RuntimeException re) {
      assertThat(re, is(RUNTIME_EXCEPTION));
    }

    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testComputeIfAbsentExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    store.put("key", "value");
    timeSource.advanceTime(1);

    store.computeIfAbsent("key", new Function<String, String>() {
      @Override
      public String apply(String mappedKey) {
        assertThat(mappedKey, equalTo("key"));
        return "value2";
      }
    });

    assertThat(store.get("key").value(), equalTo("value2"));
  }

  @Test
  public void testExpiryCreateException() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, new Expiry<String, Object>() {

      @Override
      public Duration getExpiryForCreation(String key, Object value) {
        throw RUNTIME_EXCEPTION;
      }

      @Override
      public Duration getExpiryForAccess(String key, Object value) {
        throw new AssertionError();
      }
    });

    try {
      store.put("key", "value");
      throw new AssertionError();
    } catch (RuntimeException re) {
      assertThat(re, is(RUNTIME_EXCEPTION));
    }

    assertThat(store.get("key"), nullValue());
  }

  public void testExpiryAccessException() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, new Expiry<String, Object>() {

      @Override
      public Duration getExpiryForCreation(String key, Object value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForAccess(String key, Object value) {
        throw RUNTIME_EXCEPTION;
      }
    });

    store.put("key", "value");

    try {
      store.get("key");
      throw new AssertionError();
    } catch (RuntimeException re) {
      assertThat(re, is(RUNTIME_EXCEPTION));
    }

    // containsKey() doesn't update access time -- shouldn't throw exception
    assertThat(store.containsKey("key"), equalTo(true));
  }

  @Test
  public void testComputeIfPresent() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    store.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        assertThat(mappedKey, equalTo("key"));
        assertThat(mappedValue, equalTo("value"));
        return "value2";
      }
    });

    assertThat(store.get("key").value(), equalTo("value2"));
  }

  @Test
  public void testComputeIfPresentMissing() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        throw new AssertionError();
      }
    });
  }

  @Test
  public void testComputeIfPresentReturnNull() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    store.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        return null;
      }
    });

    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testComputeIfPresentException() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    try {
      store.computeIfPresent("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String mappedKey, String mappedValue) {
          throw RUNTIME_EXCEPTION;
        }
      });
      throw new AssertionError();
    } catch (RuntimeException re) {
      assertThat(re, is(RUNTIME_EXCEPTION));
    }

    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testComputeIfPresentExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));

    store.put("key", "value");
    timeSource.advanceTime(1);

    store.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        throw new AssertionError();
      }
    });

    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testPutNotSerializableValue() throws Exception {
    OnHeapStore<String, Serializable> store = newStore();
    try {
      store.put("key1", new ArrayList() {{ add(new Object()); }});
      fail();
    } catch (SerializerException se) {
      // expected
    }
  }

  private static Map<String, Long> observeAccessTimes(Iterator<Entry<String, ValueHolder<String>>> iter)
      throws CacheAccessException {
    Map<String, Long> map = new HashMap<String, Long>();
    while (iter.hasNext()) {
      Entry<String, ValueHolder<String>> entry = iter.next();
      map.put(entry.getKey(), entry.getValue().lastAccessTime(TimeUnit.MILLISECONDS));
    }
    return map;
  }

  private static Map<String, String> observe(Iterator<Entry<String, ValueHolder<String>>> iter)
      throws CacheAccessException {
    Map<String, String> map = new HashMap<String, String>();
    while (iter.hasNext()) {
      Entry<String, ValueHolder<String>> entry = iter.next();
      map.put(entry.getKey(), entry.getValue().value());
    }
    return map;
  }

  private static void assertEntry(Entry<String, ValueHolder<String>> entry, String key, String value) {
    assertThat(entry.getKey(), equalTo(key));
    assertThat(entry.getValue().value(), equalTo(value));
  }

  private static class TestTimeSource implements TimeSource {

    private long time = 0;

    @Override
    public long getTimeMillis() {
      return time;
    }

    private void advanceTime(long delta) {
      this.time += delta;
    }
  }

  private <K, V> OnHeapStore<K, V> newStore() {
    return newStore(SystemTimeSource.INSTANCE, Expirations.noExpiration());
  }

  private <K, V> OnHeapStore<K, V> newStore(final TimeSource timeSource,
      final Expiry<? super String, ? super Object> expiry) {
    return new OnHeapStore<K, V>(new Store.Configuration<K, V>() {
      @Override
      public Class<K> getKeyType() {
        return (Class<K>) String.class;
      }

      @Override
      public Class<V> getValueType() {
        return (Class<V>) Serializable.class;
      }

      @Override
      public Comparable<Long> getCapacityConstraint() {
        return null;
      }

      @Override
      public Predicate<Entry<K, V>> getEvictionVeto() {
        return null;
      }

      @Override
      public Comparator<Entry<K, V>> getEvictionPrioritizer() {
        return null;
      }

      @Override
      public SerializationProvider getSerializationProvider() {
        return new JavaSerializationProvider();
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public Expiry<? super K, ? super V> getExpiry() {
        return (Expiry<? super K, ? super V>) expiry;
      }
    }, timeSource, true);
  }

}
