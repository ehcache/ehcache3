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
package org.ehcache.impl.internal.loaderwriter.writebehind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * @author Abhilash
 *
 */
public class WriteBehindTestLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {

  private final Map<K, List<V>> data = new HashMap<>();
  private CountDownLatch latch;

  public synchronized void setLatch(CountDownLatch latch) {
    this.latch = latch;
  }

  @Override
  public synchronized  V load(K key) {
    List<V> values = getValueList(key);
    if (values.isEmpty()) {
      return null;
    } else {
      return values.get(values.size() - 1);
    }
  }

  @Override
  public synchronized Map<K, V> loadAll(Iterable<? extends K> keys) {
    Map<K, V> loaded = new HashMap<>();
    for (K k : keys) {
      loaded.put(k, load(k));
    }
    return loaded;
  }

  @Override
  public synchronized void write(K key, V value) {
    getValueList(key).add(value);
    if(latch != null) latch.countDown();
  }

  @Override
  public synchronized void writeAll(Iterable<? extends Entry<? extends K, ? extends V>> entries) {
    for (Entry<? extends K, ? extends V> entry : entries) {
      write(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public synchronized void delete(K key) {
    getValueList(key).add(null);
    if(latch != null) latch.countDown();
  }

  @Override
  public synchronized void deleteAll(Iterable<? extends K> keys) {
    for (K k : keys) {
      delete(k);
    }
  }

  public synchronized Map<K, List<V>> getData() {
    return this.data;
  }

  protected List<V> getValueList(K key) {
    List<V> values = data.get(key);
    if (values == null) {
      values = new ArrayList<>();
      data.put(key, values);
    }
    return values;
  }
}


