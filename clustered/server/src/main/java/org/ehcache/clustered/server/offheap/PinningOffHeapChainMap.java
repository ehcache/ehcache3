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
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.portability.Portability;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

/**
 * This class is used in WriteBehind implementation
 */
public class PinningOffHeapChainMap<K> extends OffHeapChainMap<K> {

  public PinningOffHeapChainMap(PageSource source, Portability<? super K> keyPortability, int minPageSize, int maxPageSize, boolean shareByThieving) {
    super(source, keyPortability, minPageSize, maxPageSize, shareByThieving);
  }

  @Override
  public Chain getAndAppend(K key, ByteBuffer element) {
    return execute(key, () -> super.getAndAppend(key, element));
  }

  @Override
  public void append(K key, ByteBuffer element) {
    execute(key, () -> {
      super.append(key, element);
      return null;
    });
  }

  @Override
  public void put(K key, Chain chain) {
    execute(key, () -> {
      super.put(key, chain);
      return null;
    });
  }

  @Override
  public void replaceAtHead(K key, Chain expected, Chain replacement) {
    execute(key, () -> {
      heads.setPinning(key, false);
      super.replaceAtHead(key, expected, replacement);
      return null;
    });
  }

  private Chain execute(K key, Supplier<Chain> supplier) {
    final Lock lock = heads.writeLock();
    lock.lock();
    try {
      return supplier.get();
    } finally {
      pinIfNeeded(key);
      lock.unlock();
    }
  }

  private void pinIfNeeded(K key) {
    InternalChain internalChain = heads.get(key);
    if (internalChain != null && shouldBePinned(internalChain.detach())) {
      heads.setPinning(key, true);
    }
  }

  private boolean shouldBePinned(Chain chain) {
    for (Element element : chain) {
      if (OperationsCodec.getOperationCode(element.getPayload()).shouldBePinned()) {
        return true;
      }
    }
    return false;
  }
}
