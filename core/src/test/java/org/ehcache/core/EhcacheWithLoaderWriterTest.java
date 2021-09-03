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
package org.ehcache.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.ehcache.Cache;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.config.ResourcePoolsHelper;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author Abhilash
 *
 */
public class EhcacheWithLoaderWriterTest extends CacheTest {

  @Override
  protected InternalCache<Object, Object> getCache(Store<Object, Object> store) {
    final CacheConfiguration<Object, Object> config = new BaseCacheConfiguration<>(Object.class, Object.class, null,
      null, null, ResourcePoolsHelper.createHeapOnlyPools());
    @SuppressWarnings("unchecked")
    CacheEventDispatcher<Object, Object> cacheEventDispatcher = mock(CacheEventDispatcher.class);
    @SuppressWarnings("unchecked")
    CacheLoaderWriter<Object, Object> cacheLoaderWriter = mock(CacheLoaderWriter.class);
    @SuppressWarnings("unchecked")
    ResilienceStrategy<Object, Object> resilienceStrategy = mock(ResilienceStrategy.class);
    return new EhcacheWithLoaderWriter<>(config, store, resilienceStrategy, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory.getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheWithLoaderWriterTest"));
  }

  @Test
  public void testIgnoresKeysReturnedFromCacheLoaderLoadAll() {
    LoadAllVerifyStore store = new LoadAllVerifyStore();
    @SuppressWarnings("unchecked")
    ResilienceStrategy<String, String> resilienceStrategy = mock(ResilienceStrategy.class);
    KeyFumblingCacheLoaderWriter loader = new KeyFumblingCacheLoaderWriter();
    @SuppressWarnings("unchecked")
    CacheEventDispatcher<String, String> cacheEventDispatcher = mock(CacheEventDispatcher.class);
    CacheConfiguration<String, String> config = new BaseCacheConfiguration<>(String.class, String.class, null,
      null, null, ResourcePoolsHelper.createHeapOnlyPools());
    EhcacheWithLoaderWriter<String, String> ehcache = new EhcacheWithLoaderWriter<>(config, store, resilienceStrategy, loader, cacheEventDispatcher, LoggerFactory
      .getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheTest6"));
    ehcache.init();

    HashSet<String> keys = new HashSet<>();
    keys.add("key1");
    keys.add("key2");
    keys.add("key3");
    keys.add("key4");

    ehcache.getAll(keys);
    assertTrue("validation performed inline by LoadAllVerifyStore", true);
  }

  private static class LoadAllVerifyStore implements Store<String, String> {

    @Override
    public Map<String, ValueHolder<String>> bulkComputeIfAbsent(Set<? extends String> keys, Function<Iterable<? extends String>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> mappingFunction) throws StoreAccessException {
      Iterable<? extends Map.Entry<? extends String, ? extends String>> result = mappingFunction.apply(keys);
      ArrayList<String> functionReturnedKeys = new ArrayList<>();
      for (Map.Entry<? extends String, ? extends String> entry : result) {
        functionReturnedKeys.add(entry.getKey());
      }
      assertThat(functionReturnedKeys.size(), is(keys.size()));

      ArrayList<String> paramKeys = new ArrayList<>(keys);
      Collections.sort(paramKeys);
      Collections.sort(functionReturnedKeys);

      for (int i = 0; i < functionReturnedKeys.size(); i++) {
        assertThat(functionReturnedKeys.get(i), sameInstance(paramKeys.get(i)));
      }

      return Collections.emptyMap();
    }

    @Override
    public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
      return new ArrayList<>();
    }

    @Override
    public ValueHolder<String> get(String key) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public boolean containsKey(String key) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public PutStatus put(String key, String value) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ValueHolder<String> putIfAbsent(String key, String value) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public boolean remove(String key) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public RemoveStatus remove(String key, String value) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ValueHolder<String> replace(String key, String value) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ReplaceStatus replace(String key, String oldValue, String newValue) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void clear() throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public StoreEventSource<String, String> getStoreEventSource() {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public Iterator<Cache.Entry<String, ValueHolder<String>>> iterator() {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ValueHolder<String> compute(String key, BiFunction<? super String, ? super String, ? extends String> mappingFunction) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ValueHolder<String> compute(String key, BiFunction<? super String, ? super String, ? extends String> mappingFunction, Supplier<Boolean> replaceEqual) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public ValueHolder<String> computeIfAbsent(String key, Function<? super String, ? extends String> mappingFunction) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public Map<String, ValueHolder<String>> bulkCompute(Set<? extends String> keys, Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> remappingFunction) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public Map<String, ValueHolder<String>> bulkCompute(Set<? extends String> keys, Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> remappingFunction, Supplier<Boolean> replaceEqual) throws StoreAccessException {
      throw new UnsupportedOperationException("TODO Implement me!");
    }
  }

  private static class KeyFumblingCacheLoaderWriter implements CacheLoaderWriter<String, String> {
    @Override
    public Map<String, String> loadAll(Iterable<? extends String> keys) {
      HashMap<String, String> result = new HashMap<>();
      for (String key : keys) {
        result.put(new String(key), "valueFor" + key);
      }
      return result;
    }

    @Override
    public void write(String key, String value) {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void writeAll(Iterable<? extends Map.Entry<? extends String, ? extends String>> entries) {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void delete(String key) {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void deleteAll(Iterable<? extends String> keys) {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public String load(String key) {
      throw new UnsupportedOperationException("TODO Implement me!");
    }
  }

}
