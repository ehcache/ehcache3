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
package org.ehcache.clustered.server.store.impl;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.ServerStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implements {@link ServerStore}
 */
public class ReferenceStoreImpl implements ServerStore  {

  private final Map<Long, Chain> map = new HashMap<Long, Chain>();
  private final List<ReadWriteLock> locks = new ArrayList<ReadWriteLock>();
  private final AtomicLong sequenceGenerator = new AtomicLong();

  private final int LOCK_COUNT = 16;

  public ReferenceStoreImpl() {
    for (int i = 0; i < LOCK_COUNT; i++) {
      locks.add(new ReentrantReadWriteLock());
    }
  }

  private ReadWriteLock getLock(long key) {
    return locks.get((int)Math.abs(key)%LOCK_COUNT);
  }

  @Override
  public Chain get(long key) {
    Lock lock = getLock(key).readLock();
    lock.lock();
    try {
      Chain chain = map.get(key);
      if (chain != null) {
        return chain;
      } else {
        return new HeapChainImpl();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void append(long key, ByteBuffer payLoad) {
    Lock lock =  getLock(key).writeLock();
    lock.lock();
    try {
      Chain mapping = map.get(key);
      if (mapping == null) {
        map.put(key, new HeapChainImpl(new HeapElementImpl(sequenceGenerator.incrementAndGet(), payLoad)));
        return;
      }
      Chain newMapping = cast(mapping).append(new HeapElementImpl(sequenceGenerator.incrementAndGet(), payLoad));
      map.put(key, newMapping);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Chain getAndAppend(long key, ByteBuffer payLoad) {
    Lock lock =  getLock(key).writeLock();
    lock.lock();
    try {
      Chain mapping = map.get(key);
      if (mapping != null) {
        Chain newMapping = cast(mapping).append(new HeapElementImpl(sequenceGenerator.incrementAndGet(), payLoad));
        map.put(key, newMapping);
        return mapping;
      } else {
        map.put(key, new HeapChainImpl(new HeapElementImpl(sequenceGenerator.incrementAndGet(), payLoad)));
        return new HeapChainImpl();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    Lock lock =  getLock(key).writeLock();
    lock.lock();
    try {
      Chain mapping = map.get(key);
      if (mapping == null) {
        return;
      }
      boolean replaceable = true;
      List<Element> elements = new LinkedList<Element>();
      Iterator<Element> current = mapping.iterator();
      Iterator<Element> expected = expect.iterator();
      while (expected.hasNext()) {
        if (current.hasNext()) {
          HeapElementImpl expectedLink = (HeapElementImpl)expected.next();
          if (expectedLink.getSequenceNumber() != ((HeapElementImpl)current.next()).getSequenceNumber()) {
            replaceable = false;
            break;
          }
        } else {
          replaceable = false;
          break;
        }
      }

      if (replaceable) {
        for (Element element : update) {
          elements.add(element);
        }
        while(current.hasNext()) {
          elements.add(current.next());
        }
        map.put(key, new HeapChainImpl(elements.toArray(new Element[elements.size()])));
      }

    } finally {
      lock.unlock();
    }
  }

  private void writeLockAll() {
    for (ReadWriteLock lock : locks) {
      lock.writeLock().lock();
    }
  }

  private void writeUnlockAll() {
    for (ReadWriteLock lock : locks) {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void clear() {
    writeLockAll();
    try {
      map.clear();
    } finally {
      writeUnlockAll();
    }
  }

  private HeapChainImpl cast(Chain chain) {
    return (HeapChainImpl)chain;
  }

}
