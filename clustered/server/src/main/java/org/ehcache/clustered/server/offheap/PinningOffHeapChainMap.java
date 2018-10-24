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

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.server.offheap.InternalChain.ReplaceResponse;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.portability.Portability;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

/**
 * This class is used in WriteBehind implementation
 */
public class PinningOffHeapChainMap<K> extends OffHeapChainMap<K> {

  public PinningOffHeapChainMap(PageSource source, Portability<? super K> keyPortability, int minPageSize, int maxPageSize, boolean shareByThieving) {
    super(source, keyPortability, minPageSize, maxPageSize, shareByThieving);
  }

  public Chain getAndAppend(K key, ByteBuffer element) {
    final Lock lock = heads.writeLock();
    lock.lock();
    try {
      while (true) {
        InternalChain chain = heads.get(key);
        if (chain == null) {
          heads.putPinned(key, chainStorage.newChain(element));
          return EMPTY_CHAIN;
        } else {
          try {
            Chain current = chain.detach();
            if (chain.append(element)) {
              heads.setPinning(key, true);
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
          heads.putPinned(key, chainStorage.newChain(element));
          return;
        } else {
          try {
            if (chain.append(element)) {
              heads.setPinning(key, true);
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
    replaceAtHead(key, expected, replacement, true);
  }

  public void put(K key, Chain chain) {
    final Lock lock = heads.writeLock();
    lock.lock();
    try {
      InternalChain current = heads.get(key);
      if (current != null) {
        try {
          replaceAtHead(key, current.detach(), chain, false);
        } finally {
          current.close();
        }
      } else {
        if (!chain.isEmpty()) {
          heads.putPinned(key, chainStorage.newChain(chain));
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void replaceAtHead(K key, Chain expected, Chain replacement, boolean shouldUnpin) {
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
            heads.setPinning(key, false);
            ReplaceResponse response = chain.replace(expected, replacement);
            if (response != ReplaceResponse.MATCH_BUT_NOT_REPLACED) {
              if (!shouldUnpin || response != ReplaceResponse.EXACT_MATCH_AND_REPLACED) {
                heads.setPinning(key, true);
              }
              return;
            } else {
              heads.setPinning(key, true);
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
}
