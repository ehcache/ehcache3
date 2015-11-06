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

package org.ehcache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.spi.LifeCycled;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class EhcacheTest {

  @Test
  public void testTransistionsState() {
    Store store = mock(Store.class);
    final CacheConfiguration<Object, Object> config = newCacheConfigurationBuilder()
        .buildConfig(Object.class, Object.class);
    Ehcache ehcache = new Ehcache(config, store, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheTest"));
    assertThat(ehcache.getStatus(), is(Status.UNINITIALIZED));
    ehcache.init();
    assertThat(ehcache.getStatus(), is(Status.AVAILABLE));
    ehcache.close();
    assertThat(ehcache.getStatus(), is(Status.UNINITIALIZED));
  }

  @Test
  public void testThrowsWhenNotAvailable() throws CacheAccessException {
    Store store = mock(Store.class);
    Store.Iterator mockIterator = mock(Store.Iterator.class);
    when(store.iterator()).thenReturn(mockIterator);
    final CacheConfiguration<Object, Object> config = newCacheConfigurationBuilder()
        .buildConfig(Object.class, Object.class);
    Ehcache ehcache = new Ehcache(config, store, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheTest1"));

    try {
      ehcache.get("foo");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.put("foo", "bar");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.remove("foo");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.remove("foo", "bar");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.containsKey("foo");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.replace("foo", "bar");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.replace("foo", "foo", "bar");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.putIfAbsent("foo", "bar");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.clear();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.iterator();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.getAll(Collections.singleton("foo"));
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.removeAll(Collections.singleton("foo"));
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    try {
      ehcache.putAll(Collections.singletonMap("foo", "bar"));
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }

    ehcache.init();
    final Iterator iterator = ehcache.iterator();
    ehcache.close();
    try {
      iterator.hasNext();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }
    try {
      iterator.next();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }
    try {
      iterator.remove();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }
  }

  @Test
  public void testDelegatesLifecycleCallsToStore() throws Exception {
    final CacheConfiguration<Object, Object> config = newCacheConfigurationBuilder()
        .buildConfig(Object.class, Object.class);
    Ehcache ehcache = new Ehcache(config, mock(Store.class), LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheTest2"));
    final LifeCycled mock = mock(LifeCycled.class);
    ehcache.addHook(mock);
    ehcache.init();
    verify(mock).init();
    ehcache.close();
    verify(mock).close();
  }

  @Test
  public void testFailingTransitionGoesToLowestStatus() throws Exception {
    final LifeCycled mock = mock(LifeCycled.class);
    final CacheConfiguration<Object, Object> config = newCacheConfigurationBuilder()
        .buildConfig(Object.class, Object.class);
    Ehcache ehcache = new Ehcache(config, mock(Store.class), LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheTest3"));
    doThrow(new Exception()).when(mock).init();
    ehcache.addHook(mock);
    try {
      ehcache.init();
      fail();
    } catch (StateTransitionException e) {
      assertThat(ehcache.getStatus(), is(Status.UNINITIALIZED));
    }

    reset(mock);
    ehcache.init();
    assertThat(ehcache.getStatus(), is(Status.AVAILABLE));
    ehcache.close();
  }
  
  @Test
  public void testPutIfAbsent() throws CacheAccessException {
    final AtomicReference<Object> existingValue = new AtomicReference<Object>();
    final Store store = mock(Store.class);
    final String value = "bar";
    when(store.computeIfAbsent(eq("foo"), any(Function.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
        final Function<Object, Object> biFunction
            = (Function<Object, Object>)invocationOnMock.getArguments()[1];
        if (existingValue.get() == null) {
          final Object newValue = biFunction.apply(invocationOnMock.getArguments()[0]);
          existingValue.compareAndSet(null, newValue);
        }
        return new Store.ValueHolder<Object>() {
          @Override
          public Object value() {
            return existingValue.get();
          }

          @Override
          public long creationTime(final TimeUnit unit) {
            throw new UnsupportedOperationException("Implement me!");
          }

          @Override
          public long expirationTime(TimeUnit unit) {
            throw new UnsupportedOperationException("Implement me!");
          }

          @Override
          public boolean isExpired(long expirationTime, TimeUnit unit) {
            throw new UnsupportedOperationException("Implement me!");
          }

          @Override
          public long lastAccessTime(final TimeUnit unit) {
            throw new UnsupportedOperationException("Implement me!");
          }

          @Override
          public float hitRate(final long now, final TimeUnit unit) {
            throw new UnsupportedOperationException("Implement me!");
          }

          @Override
          public long hits() {
            throw new UnsupportedOperationException("Implement me!");
          }

          @Override
          public long getId() {
            throw new UnsupportedOperationException("Implement me!");
          }
        };
      }
    });
    final CacheConfiguration<Object, Object> config = newCacheConfigurationBuilder()
        .buildConfig(Object.class, Object.class);
    Ehcache<Object, Object> ehcache = new Ehcache<Object, Object>(
        config, store, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheTest4"));
    ehcache.init();
    assertThat(ehcache.putIfAbsent("foo", value), nullValue());
    assertThat(ehcache.putIfAbsent("foo", "foo"), CoreMatchers.<Object>is(value));
    assertThat(ehcache.putIfAbsent("foo", "foobar"), CoreMatchers.<Object>is(value));
    assertThat(ehcache.putIfAbsent("foo", value), CoreMatchers.<Object>is(value));
  }

  @Test
  public void testInvokesHooks() {
    Store store = mock(Store.class);
    final CacheConfiguration<Object, Object> config = newCacheConfigurationBuilder()
        .buildConfig(Object.class, Object.class);
    Ehcache ehcache = new Ehcache(config, store, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheTest5"));

    final LifeCycled hook = mock(LifeCycled.class);
    ehcache.addHook(hook);
    ehcache.init();
    try {
      verify(hook).init();
    } catch (Exception e) {
      fail();
    }
    reset(hook);
    try {
      ehcache.removeHook(hook);
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
    ehcache.close();
    try {
      verify(hook).close();
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testIgnoresKeysReturnedFromCacheLoaderLoadAll() {
    LoadAllVerifyStore store = new LoadAllVerifyStore();
    KeyFumblingCacheLoaderWriter loader = new KeyFumblingCacheLoaderWriter();
    Ehcache<String, String> ehcache = new Ehcache<String, String>(newCacheConfigurationBuilder()
        .buildConfig(String.class, String.class), store, loader, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheTest6"));
    ehcache.init();

    HashSet<String> keys = new HashSet<String>();
    keys.add("key1");
    keys.add("key2");
    keys.add("key3");
    keys.add("key4");

    ehcache.getAll(keys);
    assertTrue("validation performed inline by LoadAllVerifyStore", true);
  }

  private static class LoadAllVerifyStore implements Store<String, String> {

    @Override
    public Map<String, ValueHolder<String>> bulkComputeIfAbsent(Set<? extends String> keys, Function<Iterable<? extends String>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> mappingFunction) throws CacheAccessException {
      Iterable<? extends Map.Entry<? extends String, ? extends String>> result = mappingFunction.apply(keys);
      ArrayList<String> functionReturnedKeys = new ArrayList<String>();
      for (Map.Entry<? extends String, ? extends String> entry : result) {
        functionReturnedKeys.add(entry.getKey());
      }
      assertThat(functionReturnedKeys.size(), is(keys.size()));

      ArrayList<String> paramKeys = new ArrayList<String>(keys);
      Collections.sort(paramKeys);
      Collections.sort(functionReturnedKeys);

      for (int i = 0; i < functionReturnedKeys.size(); i++) {
        assertThat(functionReturnedKeys.get(i), sameInstance(paramKeys.get(i)));
      }

      return Collections.emptyMap();
    }

    @Override
    public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
      return new ArrayList<CacheConfigurationChangeListener>();
    }

    @Override
    public ValueHolder<String> get(String key) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public boolean containsKey(String key) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void put(String key, String value) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ValueHolder<String> putIfAbsent(String key, String value) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void remove(String key) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public boolean remove(String key, String value) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ValueHolder<String> replace(String key, String value) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public boolean replace(String key, String oldValue, String newValue) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void clear() throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void enableStoreEventNotifications(StoreEventListener<String, String> listener) {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void disableStoreEventNotifications() {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public Iterator<Cache.Entry<String, ValueHolder<String>>> iterator() {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ValueHolder<String> compute(String key, BiFunction<? super String, ? super String, ? extends String> mappingFunction) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ValueHolder<String> compute(String key, BiFunction<? super String, ? super String, ? extends String> mappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ValueHolder<String> computeIfAbsent(String key, Function<? super String, ? extends String> mappingFunction) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ValueHolder<String> computeIfPresent(String key, BiFunction<? super String, ? super String, ? extends String> remappingFunction) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ValueHolder<String> computeIfPresent(String key, BiFunction<? super String, ? super String, ? extends String> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public Map<String, ValueHolder<String>> bulkCompute(Set<? extends String> keys, Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> remappingFunction) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public Map<String, ValueHolder<String>> bulkCompute(Set<? extends String> keys, Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }
  }

  private static class KeyFumblingCacheLoaderWriter implements CacheLoaderWriter<String, String> {
    @Override
    public Map<String, String> loadAll(Iterable<? extends String> keys) throws Exception {
      HashMap<String, String> result = new HashMap<String, String>();
      for (String key : keys) {
        result.put(new String(key), "valueFor" + key);
      }
      return result;
    }

    @Override
    public void write(String key, String value) throws Exception {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void writeAll(Iterable<? extends Map.Entry<? extends String, ? extends String>> entries) throws BulkCacheWritingException, Exception {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void delete(String key) throws Exception {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void deleteAll(Iterable<? extends String> keys) throws BulkCacheWritingException, Exception {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public String load(String key) throws Exception {
      throw new UnsupportedOperationException("TODO Implement me!");
    }
  }
}
