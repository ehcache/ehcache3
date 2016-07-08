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
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.ServerStoreMessageFactory;
import org.ehcache.clustered.common.internal.store.Chain;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;

/**
 * Provides client-side access to the services of a {@code ServerStore}.
 */
class NoInvalidationServerStoreProxy implements ServerStoreProxy {

  private final ServerStoreMessageFactory messageFactory;
  private final EhcacheClientEntity entity;

  NoInvalidationServerStoreProxy(ServerStoreMessageFactory messageFactory, final EhcacheClientEntity entity) {
    this.messageFactory = messageFactory;
    this.entity = entity;
  }

  @Override
  public String getCacheId() {
    return messageFactory.getCacheId();
  }

  @Override
  public void addInvalidationListener(InvalidationListener listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeInvalidationListener(InvalidationListener listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Chain get(long key) throws TimeoutException {
    EhcacheEntityResponse response;
    try {
      response = entity.invoke(messageFactory.getOperation(key), false);
    } catch (TimeoutException e) {
      throw e;
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
    if (response != null && response.getType() == EhcacheEntityResponse.Type.GET_RESPONSE) {
      return ((EhcacheEntityResponse.GetResponse)response).getChain();
    } else {
      throw new ServerStoreProxyException("Response for get operation was invalid : " +
                                          (response != null ? response.getType().toString() : "null message"));
    }
  }

  @Override
  public void append(long key, ByteBuffer payLoad) throws TimeoutException {
    try {
      entity.invoke(messageFactory.appendOperation(key, payLoad), true);
    } catch (TimeoutException e) {
      throw e;
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
  }

  @Override
  public Chain getAndAppend(long key, ByteBuffer payLoad) throws TimeoutException {
    EhcacheEntityResponse response;
    try {
      response = entity.invoke(messageFactory.getAndAppendOperation(key, payLoad), true);
    } catch (TimeoutException e) {
      throw e;
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
    if (response != null && response.getType() == EhcacheEntityResponse.Type.GET_RESPONSE) {
      return ((EhcacheEntityResponse.GetResponse)response).getChain();
    } else {
      throw new ServerStoreProxyException("Response for getAndAppend operation was invalid : " +
                                          (response != null ? response.getType().toString() : "null message"));
    }
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    // TODO: Optimize this method to just send sequences for expect Chain
    try {
      entity.invokeAsync(messageFactory.replaceAtHeadOperation(key, expect, update), true);
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
  }

  @Override
  public void clear() throws TimeoutException {
    try {
      entity.invoke(messageFactory.clearOperation(), true);
    } catch (TimeoutException e) {
      throw e;
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
  }
}
