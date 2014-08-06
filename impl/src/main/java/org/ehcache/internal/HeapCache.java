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

package org.ehcache.internal;

import org.ehcache.Cache;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

/**
 * @author cdennis
 */
public class HeapCache<K, V> implements Cache<K, V> {

  private final Map<K, V> underlying = new ConcurrentHashMap<K, V>();

  @Override
  public V get(K key) {
    return underlying.get(key);
  }

  @Override
  public void put(K key, V value) {
    underlying.put(key, value);
  }

  @Override
  public V getAndRemove(K key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean replace(final K key, final V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public V getAndReplace(final K key, final V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public V getAndPut(K key, V value) {
    return underlying.put(key, value);
  }

  @Override
  public Map<K, V> getAll(Set<? extends K> keys) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeAll(Set<? extends K> keys) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeAll() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public <C extends Configuration<K, V>> C getConfiguration(final Class<C> cClass) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public <T> T invoke(final K k, final EntryProcessor<K, V, T> kvtEntryProcessor, final Object... objects) throws EntryProcessorException {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public <T> Map<K, EntryProcessorResult<T>> invokeAll(final Set<? extends K> ks, final EntryProcessor<K, V, T> kvtEntryProcessor, final Object... objects) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public CacheManager getCacheManager() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public <T> T unwrap(final Class<T> tClass) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void registerCacheEntryListener(final CacheEntryListenerConfiguration<K, V> kvCacheEntryListenerConfiguration) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void deregisterCacheEntryListener(final CacheEntryListenerConfiguration<K, V> kvCacheEntryListenerConfiguration) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> entries) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean putIfAbsent(final K key, final V value) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean remove(final K key) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean remove(final K key, final V oldValue) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean containsKey(K key) {
    return underlying.containsKey(key);
  }

  @Override
  public void loadAll(final Set<? extends K> ks, final boolean b, final CompletionListener completionListener) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    throw new UnsupportedOperationException("Implement me!");
  }
}
