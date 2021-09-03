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

package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.client.internal.store.lock.LockingServerStoreProxy;
import org.ehcache.clustered.common.internal.store.Chain;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

public class FailedReconnectStoreProxy implements LockingServerStoreProxy {
  private final Throwable failure;
  private final String cacheId;

  public FailedReconnectStoreProxy(String cacheId, Throwable failure) {
    this.cacheId = cacheId;
    this.failure = failure;
  }

  @Override
  public ChainEntry get(long key) {
    throw new RuntimeException("Cache " + getCacheId() + " failed reconnecting to cluster", failure);
  }

  @Override
  public void append(long key, ByteBuffer payLoad) {
    throw new RuntimeException("Cache " + getCacheId() + " failed reconnecting to cluster", failure);
  }

  @Override
  public ChainEntry getAndAppend(long key, ByteBuffer payLoad) {
    throw new RuntimeException("Cache " + getCacheId() + " failed reconnecting to cluster", failure);
  }

  @Override
  public void enableEvents(boolean enable) {
    //do nothing
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    throw new RuntimeException("Cache " + getCacheId() + " failed reconnecting to cluster", failure);
  }

  @Override
  public void clear() {
    throw new RuntimeException("Cache " + getCacheId() + " failed reconnecting to cluster", failure);
  }

  @Override
  public Iterator<Chain> iterator() {
    throw new RuntimeException("Cache " + getCacheId() + " failed reconnecting to cluster", failure);
  }

  @Override
  public String getCacheId() {
    return cacheId;
  }

  @Override
  public void close() {
    //ignore
  }

  @Override
  public ChainEntry lock(long hash) {
    throw new RuntimeException("Cache " + getCacheId() + " failed reconnecting to cluster", failure);
  }

  @Override
  public void unlock(long hash, boolean localonly) {
    throw new RuntimeException("Cache " + getCacheId() + " failed reconnecting to cluster", failure);
  }
}
