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

import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheResponseType;
import org.ehcache.clustered.common.internal.messages.ServerStoreMessageFactory;
import org.ehcache.clustered.common.internal.store.Chain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;

/**
 * Provides client-side access to the services of a {@code ServerStore}.
 */
class CommonServerStoreProxy implements ServerStoreProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommonServerStoreProxy.class);

  private final String cacheId;
  private final ServerStoreMessageFactory messageFactory;
  private final ClusterTierClientEntity entity;

  private final List<InvalidationListener> invalidationListeners = new CopyOnWriteArrayList<InvalidationListener>();
  private final Map<Class<? extends EhcacheEntityResponse>, SimpleClusterTierClientEntity.ResponseListener<? extends EhcacheEntityResponse>> responseListeners
      = new ConcurrentHashMap<Class<? extends EhcacheEntityResponse>, SimpleClusterTierClientEntity.ResponseListener<? extends EhcacheEntityResponse>>();

  CommonServerStoreProxy(final String cacheId, final ServerStoreMessageFactory messageFactory, final ClusterTierClientEntity entity) {
    this.cacheId = cacheId;
    this.messageFactory = messageFactory;
    this.entity = entity;
    this.responseListeners.put(EhcacheEntityResponse.ServerInvalidateHash.class, new SimpleClusterTierClientEntity.ResponseListener<EhcacheEntityResponse.ServerInvalidateHash>() {
      @Override
      public void onResponse(EhcacheEntityResponse.ServerInvalidateHash response) {
        long key = response.getKey();
        LOGGER.debug("CLIENT: on cache {}, server requesting hash {} to be invalidated", cacheId, key);
        for (InvalidationListener listener : invalidationListeners) {
          listener.onInvalidateHash(key);
        }
      }
    });
    this.responseListeners.put(EhcacheEntityResponse.ClientInvalidateHash.class, new SimpleClusterTierClientEntity.ResponseListener<EhcacheEntityResponse.ClientInvalidateHash>() {
      @Override
      public void onResponse(EhcacheEntityResponse.ClientInvalidateHash response) {
        final long key = response.getKey();
        final int invalidationId = response.getInvalidationId();

        LOGGER.debug("CLIENT: doing work to invalidate hash {} from cache {} (ID {})", key, cacheId, invalidationId);
        for (InvalidationListener listener : invalidationListeners) {
          listener.onInvalidateHash(key);
        }

        try {
          LOGGER.debug("CLIENT: ack'ing invalidation of hash {} from cache {} (ID {})", key, cacheId, invalidationId);
          entity.invokeServerStoreOperationAsync(messageFactory.clientInvalidationAck(key, invalidationId), false);
        } catch (Exception e) {
          //TODO: what should be done here?
          LOGGER.error("error acking client invalidation of hash {} on cache {}", key, cacheId, e);
        }
      }
    });
    this.responseListeners.put(EhcacheEntityResponse.ClientInvalidateAll.class, new SimpleClusterTierClientEntity.ResponseListener<EhcacheEntityResponse.ClientInvalidateAll>() {
      @Override
      public void onResponse(EhcacheEntityResponse.ClientInvalidateAll response) {
        final int invalidationId = response.getInvalidationId();

        LOGGER.debug("CLIENT: doing work to invalidate all from cache {} (ID {})", cacheId, invalidationId);
        for (InvalidationListener listener : invalidationListeners) {
          listener.onInvalidateAll();
        }

        try {
          LOGGER.debug("CLIENT: ack'ing invalidation of all from cache {} (ID {})", cacheId, invalidationId);
          entity.invokeServerStoreOperationAsync(messageFactory.clientInvalidationAllAck(invalidationId), false);
        } catch (Exception e) {
          //TODO: what should be done here?
          LOGGER.error("error acking client invalidation of all on cache {}", cacheId, e);
        }
      }
    });

    addResponseListenersToEntity();
  }

  @SuppressWarnings("unchecked")
  private void addResponseListenersToEntity() {
    for (Map.Entry<Class<? extends EhcacheEntityResponse>, SimpleClusterTierClientEntity.ResponseListener<? extends EhcacheEntityResponse>> classResponseListenerEntry :
        this.responseListeners.entrySet()) {
      this.entity.addResponseListener(classResponseListenerEntry.getKey(), (SimpleClusterTierClientEntity.ResponseListener)classResponseListenerEntry.getValue());
    }
  }

  @Override
  public String getCacheId() {
    return cacheId;
  }

  @Override
  public void addInvalidationListener(InvalidationListener listener) {
    invalidationListeners.add(listener);
  }

  @Override
  public boolean removeInvalidationListener(InvalidationListener listener) {
    return invalidationListeners.remove(listener);
  }

  <T extends EhcacheEntityResponse> void addResponseListeners(Class<T> listenerClass, SimpleClusterTierClientEntity.ResponseListener<T> listener) {
    this.responseListeners.put(listenerClass, listener);
    this.entity.addResponseListener(listenerClass, listener);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void close() {
    entity.close();
  }

  @Override
  public Chain get(long key) throws TimeoutException {
    EhcacheEntityResponse response;
    try {
      response = entity.invokeServerStoreOperation(messageFactory.getOperation(key), false);
    } catch (TimeoutException e) {
      throw e;
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
    if (response != null && response.getResponseType() == EhcacheResponseType.GET_RESPONSE) {
      return ((EhcacheEntityResponse.GetResponse)response).getChain();
    } else {
      throw new ServerStoreProxyException("Response for get operation was invalid : " +
                                          (response != null ? response.getResponseType() : "null message"));
    }
  }

  @Override
  public void append(long key, ByteBuffer payLoad) throws TimeoutException {
    try {
      entity.invokeServerStoreOperation(messageFactory.appendOperation(key, payLoad), true);
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
      response = entity.invokeServerStoreOperation(messageFactory.getAndAppendOperation(key, payLoad), true);
    } catch (TimeoutException e) {
      throw e;
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
    if (response != null && response.getResponseType() == EhcacheResponseType.GET_RESPONSE) {
      return ((EhcacheEntityResponse.GetResponse)response).getChain();
    } else {
      throw new ServerStoreProxyException("Response for getAndAppend operation was invalid : " +
                                          (response != null ? response.getResponseType() : "null message"));
    }
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    // TODO: Optimize this method to just send sequences for expect Chain
    try {
      entity.invokeServerStoreOperationAsync(messageFactory.replaceAtHeadOperation(key, expect, update), false);
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
  }

  @Override
  public void clear() throws TimeoutException {
    try {
      entity.invokeServerStoreOperation(messageFactory.clearOperation(), true);
    } catch (TimeoutException e) {
      throw e;
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
  }
}
