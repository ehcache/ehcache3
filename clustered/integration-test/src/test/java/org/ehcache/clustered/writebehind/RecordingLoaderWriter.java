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
package org.ehcache.clustered.writebehind;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordingLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {

  private final Map<K, List<V>> records = new HashMap<>();

  @Override
  public synchronized V load(K key) {
    List<V> list = records.get(key);
    return list == null ? null : list.get(list.size() - 1);
  }

  @Override
  public synchronized void write(K key, V value) {
    record(key, value);
  }

  @Override
  public synchronized void delete(K key) {
    record(key, null);
  }

  @Override
  public synchronized Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
    return CacheLoaderWriter.super.loadAll(keys);
  }

  @Override
  public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws Exception {
    CacheLoaderWriter.super.writeAll(entries);
  }

  @Override
  public void deleteAll(Iterable<? extends K> keys) throws Exception {
    CacheLoaderWriter.super.deleteAll(keys);
  }

  private void record(K key, V value) {
    records.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
  }

  synchronized Map<K, List<V>> getRecords() {
    return Collections.unmodifiableMap(records);
  }
}
