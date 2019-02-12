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
package org.ehcache.clustered.server.offheap;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.ehcache.clustered.common.internal.store.Chain;
import org.terracotta.offheapstore.MapInternals;

import org.terracotta.offheapstore.ReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.eviction.EvictionListener;
import org.terracotta.offheapstore.eviction.EvictionListeningReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;

import static org.ehcache.clustered.common.internal.util.ChainBuilder.chainFromList;

public class OffHeapChainMap<K> implements MapInternals, Iterable<Chain> {

  interface ChainMapEvictionListener<K> {
    void onEviction(K key);
  }

  protected final ReadWriteLockedOffHeapClockCache<K, InternalChain> heads;
  private final ChainStorageEngine<K> chainStorage;
  private volatile ChainMapEvictionListener<K> evictionListener;

  private OffHeapChainMap(PageSource source, ChainStorageEngine<K> storageEngine) {
    this.chainStorage = storageEngine;
    EvictionListener<K, InternalChain> listener = callable -> {
      try {
        Map.Entry<K, InternalChain> entry = callable.call();
        try {
          if (evictionListener != null) {
            evictionListener.onEviction(entry.getKey());
          }
        } finally {
          entry.getValue().close();
        }
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    };

    //TODO: EvictionListeningReadWriteLockedOffHeapClockCache lacks ctor that takes shareByThieving
    // this.heads = new ReadWriteLockedOffHeapClockCache<K, InternalChain>(source, shareByThieving, chainStorage);
    this.heads = new EvictionListeningReadWriteLockedOffHeapClockCache<>(listener, source, chainStorage);
  }

  public OffHeapChainMap(PageSource source, Factory<? extends ChainStorageEngine<K>> storageEngineFactory) {
    this(source, storageEngineFactory.newInstance());
  }

  public OffHeapChainMap(PageSource source, Portability<? super K> keyPortability, int minPageSize, int maxPageSize, boolean shareByThieving) {
    this(source, new OffHeapChainStorageEngine<>(source, keyPortability, minPageSize, maxPageSize, shareByThieving, shareByThieving));
  }

  //For tests
  OffHeapChainMap(ReadWriteLockedOffHeapClockCache<K, InternalChain> heads, OffHeapChainStorageEngine<K> chainStorage) {
    this.chainStorage = chainStorage;
    this.heads = heads;
  }

  void setEvictionListener(ChainMapEvictionListener<K> listener) {
    evictionListener = listener;
  }

  public ChainStorageEngine<K> getStorageEngine() {
    return chainStorage;
  }

  public Chain get(K key) {
    final Lock lock = heads.readLock();
    lock.lock();
    try {
      InternalChain chain = heads.get(key);
      if (chain == null) {
        return EMPTY_CHAIN;
      } else {
        try {
          return chain.detach();
        } finally {
          chain.close();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public Chain getAndAppend(K key, ByteBuffer element) {
    final Lock lock = heads.writeLock();
    lock.lock();
    try {
      while (true) {
        InternalChain chain = heads.get(key);
        if (chain == null) {
          heads.put(key, chainStorage.newChain(element));
          return EMPTY_CHAIN;
        } else {
          try {
            Chain current = chain.detach();
            if (chain.append(element)) {
              return current;
            } else {
              evict();
            }
          } finally {
            chain.close();
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public void append(K key, ByteBuffer element) {
    final Lock lock = heads.writeLock();
    lock.lock();
    try {
      while (true) {
        InternalChain chain = heads.get(key);
        if (chain == null) {
          heads.put(key, chainStorage.newChain(element));
          return;
        } else {
          try {
            if (chain.append(element)) {
              return;
            } else {
              evict();
            }
          } finally {
            chain.close();
          }
        }
      }
    } finally {
      lock.unlock();
    }

  }

  public void replaceAtHead(K key, Chain expected, Chain replacement) {
    final Lock lock = heads.writeLock();
    lock.lock();
    try {
      while (true) {
        InternalChain chain = heads.get(key);
        if (chain == null) {
          if (expected.isEmpty()) {
            throw new IllegalArgumentException("Empty expected sequence");
          } else {
            return;
          }
        } else {
          try {
            if (chain.replace(expected, replacement)) {
              return;
            } else {
              evict();
            }
          } finally {
            chain.close();
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public void put(K key, Chain chain) {
    final Lock lock = heads.writeLock();
    lock.lock();
    try {
      InternalChain current = heads.get(key);
      if (current != null) {
        try {
          replaceAtHead(key, current.detach(), chain);
        } finally {
          current.close();
        }
      } else {
        if (!chain.isEmpty()) {
          heads.put(key, chainStorage.newChain(chain));
        }
      }
    } finally {
      lock.unlock();
    }
  }

  void remove(K key) {
    Lock lock = heads.writeLock();
    lock.lock();
    try {
      heads.removeNoReturn(key);
    } finally {
      lock.unlock();
    }
  }

  public void clear() {
    heads.writeLock().lock();
    try {
      this.heads.clear();
    } finally {
      heads.writeLock().unlock();
    }
  }

  public Set<K> keySet() {
    heads.writeLock().lock();
    try {
      return heads.keySet();
    } finally {
      heads.writeLock().unlock();
    }
  }

  @Override
  public Iterator<Chain> iterator() {
    Iterator<Map.Entry<K, InternalChain>> headsIterator = heads.entrySet().iterator();

    return new Iterator<Chain>() {
      @Override
      public boolean hasNext() {
        return headsIterator.hasNext();
      }

      @Override
      public Chain next() {
        final Lock lock = heads.readLock();
        lock.lock();
        try {
          InternalChain chain = headsIterator.next().getValue();
          if (chain == null) {
            return EMPTY_CHAIN;
          } else {
            try {
              return chain.detach();
            } finally {
              chain.close();
            }
          }
        } finally {
          lock.unlock();
        }
      }
    };
  }

  private void evict() {
    int evictionIndex = heads.getEvictionIndex();
    if (evictionIndex < 0) {
      throw new OversizeMappingException("Storage Engine and Eviction Failed - Everything Pinned (" + getSize() + " mappings) \n" + "Storage Engine : " + chainStorage);
    } else {
      heads.evict(evictionIndex, false);
    }
  }

  private static final Chain EMPTY_CHAIN = chainFromList(Collections.emptyList());

  @Override
  public long getSize() {
    return heads.getSize();
  }

  @Override
  public long getTableCapacity() {
    return heads.getTableCapacity();
  }

  @Override
  public long getUsedSlotCount() {
    return heads.getUsedSlotCount();
  }

  @Override
  public long getRemovedSlotCount() {
    return heads.getRemovedSlotCount();
  }

  @Override
  public int getReprobeLength() {
    return heads.getReprobeLength();
  }

  @Override
  public long getAllocatedMemory() {
    return heads.getAllocatedMemory();
  }

  @Override
  public long getOccupiedMemory() {
    return heads.getOccupiedMemory();
  }

  @Override
  public long getVitalMemory() {
    return heads.getVitalMemory();
  }

  @Override
  public long getDataAllocatedMemory() {
    return heads.getDataAllocatedMemory();
  }

  @Override
  public long getDataOccupiedMemory() {
    return heads.getDataOccupiedMemory();
  }

  @Override
  public long getDataVitalMemory() {
    return heads.getDataVitalMemory();
  }

  @Override
  public long getDataSize() {
    return heads.getDataSize();
  }

  public boolean shrink() {
    return heads.shrink();
  }

  public Lock writeLock() {
    return heads.writeLock();
  }

  protected void storageEngineFailure(Object failure) {
  }

}
