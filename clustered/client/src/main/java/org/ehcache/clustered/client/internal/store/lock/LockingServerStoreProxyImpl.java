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
package org.ehcache.clustered.client.internal.store.lock;

import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

public class LockingServerStoreProxyImpl implements LockingServerStoreProxy {

  private final ServerStoreProxy storeProxy;
  private final LockManager lockManager;

  public LockingServerStoreProxyImpl(ServerStoreProxy storeProxy, LockManager lockManager) {
    this.storeProxy = storeProxy;
    this.lockManager = lockManager;
  }

  @Override
  public String getCacheId() {
    return storeProxy.getCacheId();
  }

  @Override
  public void close() {
    storeProxy.close();
  }

  @Override
  public ChainEntry lock(long key) throws TimeoutException {
    Chain chain = lockManager.lock(key);
    return new ChainEntry() {
      @Override
      public void append(ByteBuffer payLoad) throws TimeoutException {
        LockingServerStoreProxyImpl.this.append(key, payLoad);
      }

      @Override
      public void replaceAtHead(Chain equivalent) {
        LockingServerStoreProxyImpl.this.replaceAtHead(key, chain, equivalent);
      }

      @Override
      public boolean isEmpty() {
        return chain.isEmpty();
      }

      @Override
      public int length() {
        return chain.length();
      }

      @Override
      public Iterator<Element> iterator() {
        return chain.iterator();
      }
    };
  }

  @Override
  public void unlock(long key, boolean localonly) throws TimeoutException {
    lockManager.unlock(key, localonly);
  }

  @Override
  public ChainEntry get(long key) throws TimeoutException {
    return storeProxy.get(key);
  }

  @Override
  public void append(long key, ByteBuffer payLoad) throws TimeoutException {
    storeProxy.append(key, payLoad);
  }

  @Override
  public ChainEntry getAndAppend(long key, ByteBuffer payLoad) throws TimeoutException {
    return storeProxy.getAndAppend(key, payLoad);
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    storeProxy.replaceAtHead(key, expect, update);
  }

  @Override
  public void clear() throws TimeoutException {
    storeProxy.clear();
  }

  @Override
  public Iterator<Chain> iterator() throws TimeoutException {
    return storeProxy.iterator();
  }
}
