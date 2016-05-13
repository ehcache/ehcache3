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

import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.ServerStore;
import org.ehcache.core.spi.store.StoreAccessException;

import java.nio.ByteBuffer;

import static org.ehcache.clustered.common.Util.unwrapException;

/**
 * Provides client-side access to the services of a {@code ServerStore}.
 */
public class ServerStoreProxy implements ServerStore {

  private final String cacheId;
  private final EhcacheClientEntity entity;

  // TODO: Provide EhcacheClientEntity or EntityClientEndpoint to support exchange with server
  public ServerStoreProxy(String cacheId, EhcacheClientEntity entity) {
    this.cacheId = cacheId;
    this.entity = entity;
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
    EhcacheEntityResponse response = null;
    try {
      response = entity.invoke(EhcacheEntityMessage.getOperation(cacheId, key));
      if (response != null && response.getType() == EhcacheEntityResponse.Type.GET_RESPONSE) {
        return ((EhcacheEntityResponse.GetResponse)response).getChain();
      }
    } catch (Exception e) {
      unwrapException(e, StoreAccessException.class);
    }
    return null;
  }

  @Override
  public void append(long key, ByteBuffer payLoad) {
    try {
      entity.invokeCompleted(EhcacheEntityMessage.appendOperation(cacheId, key, payLoad));
    } catch (Exception e) {
      unwrapException(e, StoreAccessException.class);
    }
  }

  @Override
  public Chain getAndAppend(long key, ByteBuffer payLoad) {
    EhcacheEntityResponse response = null;
    try {
      response = entity.invokeCompleted(EhcacheEntityMessage.getAndAppendOperation(cacheId, key, payLoad));
      if (response != null && response.getType() == EhcacheEntityResponse.Type.GET_RESPONSE) {
        return ((EhcacheEntityResponse.GetResponse)response).getChain();
      }
    } catch (Exception e) {
      unwrapException(e, StoreAccessException.class);
    }
    return null;
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    try {
      entity.invokeCompleted(EhcacheEntityMessage.replaceAtHeadOperation(cacheId, key, expect, update));
    } catch (Exception e) {
      unwrapException(e, StoreAccessException.class);
    }
  }
}
