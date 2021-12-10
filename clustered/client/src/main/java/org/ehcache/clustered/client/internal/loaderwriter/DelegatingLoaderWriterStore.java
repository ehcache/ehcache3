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
package org.ehcache.clustered.client.internal.loaderwriter;

import org.ehcache.Cache;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.WrapperStore;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.spi.resilience.StoreAccessException;
import org.terracotta.context.ContextManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class DelegatingLoaderWriterStore<K, V> implements WrapperStore<K, V> {

  private final Store<K, V> delegate;

  public DelegatingLoaderWriterStore(Store<K, V> store) {
    this.delegate = store;
    ContextManager.associate(delegate).withParent(this);
  }

  @Override
  public ValueHolder<V> get(K key) throws StoreAccessException {
    return delegate.get(key);
  }

  @Override
  public boolean containsKey(K key) throws StoreAccessException {
    return delegate.containsKey(key);
  }

  @Override
  public PutStatus put(K key, V value) throws StoreAccessException {
    return delegate.put(key, value);
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value, Consumer<Boolean> put) throws StoreAccessException {
    return delegate.putIfAbsent(key, value, put);
  }

  @Override
  public boolean remove(K key) throws StoreAccessException {
    return delegate.remove(key);
  }

  @Override
  public RemoveStatus remove(K key, V value) throws StoreAccessException {
    return delegate.remove(key, value);
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws StoreAccessException {
    return delegate.replace(key, value);
  }

  @Override
  public ReplaceStatus replace(K key, V oldValue, V newValue) throws StoreAccessException {
    return delegate.replace(key, oldValue, newValue);
  }

  @Override
  public void clear() throws StoreAccessException {
    delegate.clear();
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    return new NullStoreEventDispatcher<>();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public ValueHolder<V> getAndCompute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public ValueHolder<V> computeAndGet(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter) throws StoreAccessException {
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public ValueHolder<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
    return delegate.bulkCompute(keys, remappingFunction);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, Supplier<Boolean> replaceEqual) throws StoreAccessException {
    throw new UnsupportedOperationException("Implement me");
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
    return delegate.bulkComputeIfAbsent(keys, mappingFunction);
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return Collections.emptyList();
  }

}
