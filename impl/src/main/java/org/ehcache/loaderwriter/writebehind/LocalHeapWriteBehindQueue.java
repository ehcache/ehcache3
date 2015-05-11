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
import java.util.List;

import org.ehcache.function.BiFunction;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.loaderwriter.writebehind.operations.SingleOperation;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;

/**
 * @author Geert Bevin
 * @author Tim wu
 *
 */
public class LocalHeapWriteBehindQueue<K, V> extends AbstractWriteBehindQueue<K, V> {
  
  private List<SingleOperation<K, V>> waiting = new ArrayList<SingleOperation<K, V>>();
  private final ConcurrentHashMap<K, SingleOperation<K, V>> latestOperation = new ConcurrentHashMap<K, SingleOperation<K, V>>(); 

  LocalHeapWriteBehindQueue(WriteBehindConfiguration config, CacheLoaderWriter<K, V> cacheLoaderWriter) {
    super(config, cacheLoaderWriter);
  }
  
  protected SingleOperation<K, V> getLatestOperation(K key) {
    return latestOperation.get(key);
  }
  
  protected void removeOperation(final SingleOperation<K, V> operation) {
    latestOperation.computeIfPresent(operation.getKey(), new BiFunction<K, SingleOperation<K, V>, SingleOperation<K, V>>() {

      @Override
      public SingleOperation<K, V> apply(K t, SingleOperation<K, V> oldOperation) {
        if(oldOperation == null) {
          return null; // when trying to remove non existent operation
        }
        if(oldOperation == operation) {
          return null;
        }
        return oldOperation;
      }
    });
  }
  
  @Override
  protected List<SingleOperation<K, V>> quarantineItems() {
    List<SingleOperation<K, V>> quarantined = waiting;
    waiting = new ArrayList<SingleOperation<K, V>>();
    return quarantined;
  }

  @Override
  protected void addItem(SingleOperation<K, V> operation) {
    latestOperation.put(operation.getKey(), operation);
    waiting.add(operation);    
  }

  @Override
  protected void reinsertUnprocessedItems(List<SingleOperation<K, V>> operations) {
    List<SingleOperation<K, V>> newQueue = new ArrayList<SingleOperation<K, V>>(operations);
    newQueue.addAll(waiting);
    waiting = newQueue;
  }

  @Override
  public long getQueueSize() {
    return waiting.size();
  }

}


