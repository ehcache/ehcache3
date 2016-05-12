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

import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.ServerStore;

import java.nio.ByteBuffer;

/**
 * Provides client-side access to the services of a {@code ServerStore}.
 */
public class ServerStoreProxy implements ServerStore {

  private final String cacheId;

  // TODO: Provide EhcacheClientEntity or EntityClientEndpoint to support exchange with server
  public ServerStoreProxy(String cacheId) {
    this.cacheId = cacheId;
  }

  /**
   * Gets the identifier linking a client-side cache to a {@code ServerStore} instance.
   *
   * @return the cache identifier
   */
  public String getCacheId() {
    return cacheId;
  }

  @Override
  public Chain get(long key) {
    // TODO: Implement ServerStoreProxyImpl.get
    throw new UnsupportedOperationException("ServerStoreProxyImpl.get not implemented");
  }

  @Override
  public void append(long key, ByteBuffer payLoad) {
    // TODO: Implement ServerStoreProxyImpl.append
    throw new UnsupportedOperationException("ServerStoreProxyImpl.append not implemented");
  }

  @Override
  public Chain getAndAppend(long key, ByteBuffer payLoad) {
    // TODO: Implement ServerStoreProxyImpl.getAndAppend
    throw new UnsupportedOperationException("ServerStoreProxyImpl.getAndAppend not implemented");
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    // TODO: Implement ServerStoreProxyImpl.replaceAtHead
    throw new UnsupportedOperationException("ServerStoreProxyImpl.replaceAtHead not implemented");
  }
}
