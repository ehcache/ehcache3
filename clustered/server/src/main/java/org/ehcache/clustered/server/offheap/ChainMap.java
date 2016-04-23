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
import java.util.concurrent.locks.Lock;

import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.Element;

import org.terracotta.offheapstore.ReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.paging.PageSource;

public class ChainMap<K> {

  private final ReadWriteLockedOffHeapClockCache<K, InternalChain> heads;
  private final OffHeapChainStorage chainStorage;

  public ChainMap(PageSource source) {
    this.chainStorage = new OffHeapChainStorage();
    this.heads = new ReadWriteLockedOffHeapClockCache<K, InternalChain>(source, chainStorage);
  }

  public Chain get(K key) {
    final Lock lock = heads.readLock();
    lock.lock();
    try {
      InternalChain chain = heads.get(key);
      if (chain == null) {
        return empty();
      } else {
        return chain.detach();
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
        return empty();
      } else {
        Chain current = chain.detach();
        chain.append(element);
        return current;
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
        chain.append(element);
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
        return false;
      } else {
        return chain.replace(expected, replacement);
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

  private static Chain empty() {
    return EMPTY_CHAIN;
  }
}
