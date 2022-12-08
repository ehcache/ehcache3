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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implements {@link ServerStore}
 */
public class ReferenceStoreImpl implements ServerStore  {

  private final ConcurrentMap<Long, Chain> map = new ConcurrentHashMap<>();
  private final AtomicLong sequenceGenerator = new AtomicLong();

  @Override
  public Chain get(long key) {
    return map.getOrDefault(key, new HeapChainImpl());
  }

  @Override
  public void append(long key, ByteBuffer payLoad) {
    getAndAppend(key, payLoad);
  }

  @Override
  public Chain getAndAppend(long key, ByteBuffer payLoad) {
    while (true) {
      Chain existing = map.get(key);
      if (existing == null) {
        if (map.putIfAbsent(key, new HeapChainImpl(new HeapElementImpl(sequenceGenerator.incrementAndGet(), payLoad))) == null) {
          return new HeapChainImpl();
        }
      } else {
        if (map.replace(key, existing, cast(existing).append(new HeapElementImpl(sequenceGenerator.incrementAndGet(), payLoad)))) {
          return existing;
        }
      }
    }
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    map.computeIfPresent(key, (k, existing) -> {
      Iterator<Element> current = existing.iterator();
      Iterator<Element> expected = expect.iterator();
      while (expected.hasNext()) {
        if (current.hasNext()) {
          HeapElementImpl expectedLink = (HeapElementImpl)expected.next();
          if (expectedLink.getSequenceNumber() != ((HeapElementImpl)current.next()).getSequenceNumber()) {
            return existing;
          }
        } else {
          return existing;
        }
      }

      List<Element> elements = new LinkedList<>();
      for (Element element : update) {
        elements.add(element);
      }
      while(current.hasNext()) {
        elements.add(current.next());
      }
      return new HeapChainImpl(elements.toArray(new Element[elements.size()]));
    });
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Iterator<Chain> iterator() {
    return map.values().iterator();
  }

  private HeapChainImpl cast(Chain chain) {
    return (HeapChainImpl)chain;
  }
}
