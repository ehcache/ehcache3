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

import org.ehcache.clustered.common.internal.store.Chain;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

public class EventualServerStoreProxy implements ServerStoreProxy {

  private final ServerStoreProxy delegate;

  public EventualServerStoreProxy(String cacheId, final ClusterTierClientEntity entity, final ServerCallback invalidation) {
    this.delegate = new CommonServerStoreProxy(cacheId, entity, invalidation);
  }

  @Override
  public String getCacheId() {
    return delegate.getCacheId();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public ChainEntry get(long key) throws TimeoutException {
    return delegate.get(key);
  }

  @Override
  public void append(final long key, final ByteBuffer payLoad) throws TimeoutException {
    delegate.append(key, payLoad);
  }

  @Override
  public ChainEntry getAndAppend(final long key, final ByteBuffer payLoad) throws TimeoutException {
    return delegate.getAndAppend(key, payLoad);
  }

  @Override
  public void enableEvents(boolean enable) throws TimeoutException {
    delegate.enableEvents(enable);
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    delegate.replaceAtHead(key, expect, update);
  }

  @Override
  public void clear() throws TimeoutException {
    delegate.clear();
  }

  @Override
  public Iterator<Chain> iterator() throws TimeoutException {
    return delegate.iterator();
  }
}
