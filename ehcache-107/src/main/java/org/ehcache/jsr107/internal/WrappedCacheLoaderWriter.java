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

package org.ehcache.jsr107.internal;

import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import java.util.Map;

/**
 * WrappedCacheLoaderWriter
 */
public class WrappedCacheLoaderWriter<K, V> implements Jsr107CacheLoaderWriter<K, V> {

  private final CacheLoaderWriter<K, V> delegate;

  public WrappedCacheLoaderWriter(CacheLoaderWriter<K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public Map<K, V> loadAllAlways(Iterable<? extends K> keys) throws BulkCacheLoadingException, Exception {
    return delegate.loadAll(keys);
  }

  @Override
  public V load(K key) throws Exception {
    return delegate.load(key);
  }

  @Override
  public Map<K, V> loadAll(Iterable<? extends K> keys) throws BulkCacheLoadingException, Exception {
    return delegate.loadAll(keys);
  }

  @Override
  public void write(K key, V value) throws Exception {
    delegate.write(key, value);
  }

  @Override
  public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws BulkCacheWritingException, Exception {
    delegate.writeAll(entries);
  }

  @Override
  public void delete(K key) throws Exception {
    delegate.delete(key);
  }

  @Override
  public void deleteAll(Iterable<? extends K> keys) throws BulkCacheWritingException, Exception {
    delegate.deleteAll(keys);
  }
}
