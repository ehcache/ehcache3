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
import java.util.concurrent.locks.Lock;

import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.Element;
import org.ehcache.clustered.common.store.Util;
import org.terracotta.offheapstore.MapInternals;

import org.terracotta.offheapstore.ReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.portability.Portability;

class OffHeapChainMap<K> implements MapInternals {

  private final ReadWriteLockedOffHeapClockCache<K, InternalChain> heads;
  private final OffHeapChainStorageEngine chainStorage;

  public OffHeapChainMap(PageSource source, Portability<? super K> keyPortability) {
    this.chainStorage = new OffHeapChainStorageEngine(source, keyPortability);
    this.heads = new ReadWriteLockedOffHeapClockCache<K, InternalChain>(source, chainStorage);
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
      InternalChain chain = heads.get(key);
      if (chain == null) {
        heads.put(key, chainStorage.newChain(element));
        return EMPTY_CHAIN;
      } else {
        try {
          Chain current = chain.detach();
          if (!chain.append(element)) {
            heads.remove(key).close();
          }
          return current;
        } finally {
          chain.close();
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
      InternalChain chain = heads.get(key);
      if (chain == null) {
        heads.put(key, chainStorage.newChain(element));
      } else {
        try {
          if (!chain.append(element)) {
            heads.remove(key).close();
          }
        } finally {
          chain.close();
        }
      }
    } finally {
      lock.unlock();
    }

  }

  public boolean replaceAtHead(K key, Chain expected, Chain replacement) {
    final Lock lock = heads.writeLock();
    lock.lock();
    try {
      InternalChain chain = heads.get(key);
      if (chain == null) {
        if (expected.iterator().hasNext()) {
          return false;
        } else {
          for (Element element : replacement) {
            append(key, element.getPayload());
          }
          return true;
        }
      } else {
        try {
          return chain.replace(expected, replacement);
        } finally {
          chain.close();
        }
      }
    } finally {
      lock.unlock();
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
}
