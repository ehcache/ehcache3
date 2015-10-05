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
package org.ehcache.loaderwriter.writebehind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;

/**
 * @author Abhilash
 *
 */
public class WriteBehindDecoratorLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {
  
  private final CacheLoaderWriter<K, V> delegate;
  private final AggregateWriteBehindQueue<K, V> writeBehindQueue ;
  
  public AggregateWriteBehindQueue<K, V> getWriteBehindQueue() {
    return writeBehindQueue;
  }


  public WriteBehindDecoratorLoaderWriter(CacheLoaderWriter<K, V> loaderWriter, WriteBehindConfiguration config) {
    this.delegate = loaderWriter;
    writeBehindQueue = new AggregateWriteBehindQueue<K, V>(config, loaderWriter);
    writeBehindQueue.start();
  }
  

  @Override
  public V load(K key) throws Exception {
    return writeBehindQueue.load(key); 
  }

  @Override
  public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
    Map<K, V> entries = new HashMap<K, V>();
    List<K> missingKeys = new ArrayList<K>();
    for (K k : keys) {
      V v = writeBehindQueue.load(k);
      if (v != null) entries.put(k, v) ; 
      else missingKeys.add(k) ;
    }
    // This can lead to a condition where after the key in missing list gets a write operation
    // added to the waiting queue
    Map<K, V> missingEntries = delegate.loadAll(missingKeys);
    entries.putAll(missingEntries);
    return entries;
  }
  
  @Override
  public void write(K key, V value) throws Exception {
    writeBehindQueue.write(key, value);
  }

  @Override
  public void writeAll(Iterable<? extends Entry<? extends K, ? extends V>> entries) throws BulkCacheWritingException, Exception {
    for (Entry<? extends K, ? extends V> entry : entries) 
      writeBehindQueue.write(entry.getKey(), entry.getValue());
  }

  @Override
  public void delete(K key) throws Exception {
    writeBehindQueue.delete(key);
  }

  @Override
  public void deleteAll(Iterable<? extends K> keys) throws BulkCacheWritingException, Exception {
    for (K k : keys) 
      writeBehindQueue.delete(k);
  }

}
