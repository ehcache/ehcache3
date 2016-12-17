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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.Util;
import org.terracotta.offheapstore.MapInternals;

import org.terracotta.offheapstore.ReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.eviction.EvictionListener;
import org.terracotta.offheapstore.eviction.EvictionListeningReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.portability.Portability;

import com.tc.classloader.CommonComponent;

@CommonComponent
public class OffHeapChainMap<K> implements MapInternals {

  interface ChainMapEvictionListener<K> {
    void onEviction(K key);
  }

  private final ReadWriteLockedOffHeapClockCache<K, InternalChain> heads;
  private final OffHeapChainStorageEngine<K> chainStorage;
  private volatile ChainMapEvictionListener<K> evictionListener;

  public OffHeapChainMap(PageSource source, Portability<? super K> keyPortability, int minPageSize, int maxPageSize, boolean shareByThieving) {
    this.chainStorage = new OffHeapChainStorageEngine<>(source, keyPortability, minPageSize, maxPageSize, shareByThieving, shareByThieving);
    EvictionListener<K, InternalChain> listener = new EvictionListener<K, InternalChain>() {
      @Override
      public void evicting(Callable<Map.Entry<K, InternalChain>> callable) {
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
      }
    };

    //TODO: EvictionListeningReadWriteLockedOffHeapClockCache lacks ctor that takes shareByThieving
    // this.heads = new ReadWriteLockedOffHeapClockCache<K, InternalChain>(source, shareByThieving, chainStorage);
    this.heads = new EvictionListeningReadWriteLockedOffHeapClockCache<>(listener, source, chainStorage);
  }

  //For tests
  OffHeapChainMap(ReadWriteLockedOffHeapClockCache<K, InternalChain> heads, OffHeapChainStorageEngine<K> chainStorage) {
    this.chainStorage = chainStorage;
    this.heads = heads;
  }

  void setEvictionListener(ChainMapEvictionListener<K> listener) {
    evictionListener = listener;
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
        for (Element x : chain) {
          append(key, x.getPayload());
        }
      }
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

  private void evict() {
    int evictionIndex = heads.getEvictionIndex();
    if (evictionIndex < 0) {
      StringBuilder sb = new StringBuilder("Storage Engine and Eviction Failed - Everything Pinned (");
      sb.append(getSize()).append(" mappings) \n").append("Storage Engine : ").append(chainStorage);
      throw new OversizeMappingException(sb.toString());
    } else {
      heads.evict(evictionIndex, false);
    }
  }

  private static final Chain EMPTY_CHAIN = new Chain() {
    @Override
    public Iterator<Element> reverseIterator() {
      return Collections.<Element>emptyList().iterator();
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public Iterator<Element> iterator() {
      return Collections.<Element>emptyList().iterator();
    }
  };

  public static Chain chain(ByteBuffer... buffers) {
    final List<Element> list = new ArrayList<Element>();
    for (ByteBuffer b : buffers) {
      list.add(element(b));
    }

    return new Chain() {

      final List<Element> elements = Collections.unmodifiableList(list);

      @Override
      public Iterator<Element> iterator() {
        return elements.iterator();
      }

      @Override
      public Iterator<Element> reverseIterator() {
        return Util.reverseIterator(elements);
      }

      @Override
      public boolean isEmpty() {
        return elements.isEmpty();
      }
    };
  }

  private static Element element(final ByteBuffer b) {
    return new Element() {
      @Override
      public ByteBuffer getPayload() {
        return b.asReadOnlyBuffer();
      }
    };
  }

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

  boolean shrink() {
    return heads.shrink();
  }

  Lock writeLock() {
    return heads.writeLock();
  }

  protected void storageEngineFailure(Object failure) {
  }

}
