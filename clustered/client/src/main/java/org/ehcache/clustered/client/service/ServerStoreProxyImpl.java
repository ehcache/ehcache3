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

package org.ehcache.clustered.client.service;

import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.store.Chain;

import java.nio.ByteBuffer;

/**
 * Provides a {@link ServerStoreProxy} that uses this {@link DefaultClusteringService} instance
 * for communication with the cluster server.
 *
 * @param <K> the cache-exposed key type
 * @param <V> the cache-exposed value type
 */
final class ServerStoreProxyImpl<K, V> implements ServerStoreProxy<K, V> {

  private final String cacheId;
  private final Class<K> keyType;
  private final Class<V> valueType;
  private final ServerStoreConfiguration storeConfiguration;

  ServerStoreProxyImpl(final String cacheId, final Class<K> keyType, final Class<V> valueType, ServerStoreConfiguration storeConfiguration) {
    this.cacheId = cacheId;
    this.keyType = keyType;
    this.valueType = valueType;
    this.storeConfiguration = storeConfiguration;
  }

  @Override
  public String getCacheId() {
    return cacheId;
  }

  @Override
  public Class<K> getKeyType() {
    return keyType;
  }

  @Override
  public Class<V> getValueType() {
    return valueType;
  }

  @Override
  public ServerStoreConfiguration getServerStoreConfiguration() {
    return storeConfiguration;
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
