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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;

/**
 * @author Ludovic Orban
 */
public class EventualServerStoreProxy implements ServerStoreProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventualServerStoreProxy.class);

  private final ServerStoreProxy delegate;
  private final List<InvalidationListener> invalidationListeners = new CopyOnWriteArrayList<InvalidationListener>();

  public EventualServerStoreProxy(final ServerStoreMessageFactory messageFactory, final EhcacheClientEntity entity) {
    this.delegate = new NoInvalidationServerStoreProxy(messageFactory, entity);
    entity.addResponseListener(EhcacheEntityResponse.ServerInvalidateHash.class, new EhcacheClientEntity.ResponseListener<EhcacheEntityResponse.ServerInvalidateHash>() {
      @Override
      public void onResponse(EhcacheEntityResponse.ServerInvalidateHash response) {
        if (response.getCacheId().equals(messageFactory.getCacheId())) {
          long key = response.getKey();
          LOGGER.debug("CLIENT: on cache {}, server requesting hash {} to be invalidated", messageFactory.getCacheId(), key);
          for (InvalidationListener listener : invalidationListeners) {
            listener.onInvalidateHash(key);
          }
        } else {
          LOGGER.debug("CLIENT: on cache {}, ignoring invalidation on unrelated cache : {}", messageFactory.getCacheId(), response.getCacheId());
        }
      }
    });
    entity.addResponseListener(EhcacheEntityResponse.ClientInvalidateHash.class, new EhcacheClientEntity.ResponseListener<EhcacheEntityResponse.ClientInvalidateHash>() {
      @Override
      public void onResponse(EhcacheEntityResponse.ClientInvalidateHash response) {
        final String cacheId = response.getCacheId();
        final long key = response.getKey();
        final int invalidationId = response.getInvalidationId();

        if (cacheId.equals(messageFactory.getCacheId())) {
          LOGGER.debug("CLIENT: doing work to invalidate hash {} from cache {} (ID {})", key, cacheId, invalidationId);
          for (InvalidationListener listener : invalidationListeners) {
            listener.onInvalidateHash(key);
          }
        } else {
          LOGGER.debug("CLIENT: on cache {}, ignoring invalidation on unrelated cache : {}", messageFactory.getCacheId(), response.getCacheId());
        }
      }
    });
    entity.addResponseListener(EhcacheEntityResponse.ClientInvalidateAll.class, new EhcacheClientEntity.ResponseListener<EhcacheEntityResponse.ClientInvalidateAll>() {
      @Override
      public void onResponse(EhcacheEntityResponse.ClientInvalidateAll response) {
        final String cacheId = response.getCacheId();
        final int invalidationId = response.getInvalidationId();

        if (cacheId.equals(messageFactory.getCacheId())) {
          LOGGER.debug("CLIENT: doing work to invalidate all from cache {} (ID {})", cacheId, invalidationId);
          for (InvalidationListener listener : invalidationListeners) {
            listener.onInvalidateAll();
          }
        } else {
          LOGGER.debug("CLIENT: on cache {}, ignoring invalidation on unrelated cache : {}", messageFactory.getCacheId(), response.getCacheId());
        }
      }
    });
  }

  @Override
  public String getCacheId() {
    return delegate.getCacheId();
  }

  @Override
  public void addInvalidationListener(InvalidationListener listener) {
    invalidationListeners.add(listener);
  }

  @Override
  public boolean removeInvalidationListener(InvalidationListener listener) {
    return invalidationListeners.remove(listener);
  }

  @Override
  public Chain get(long key) throws TimeoutException {
    return delegate.get(key);
  }

  @Override
  public void append(final long key, final ByteBuffer payLoad) throws TimeoutException {
    delegate.append(key, payLoad);
  }

  @Override
  public Chain getAndAppend(final long key, final ByteBuffer payLoad) throws TimeoutException {
    return delegate.getAndAppend(key, payLoad);
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    delegate.replaceAtHead(key, expect, update);
  }

  @Override
  public void clear() throws TimeoutException {
    delegate.clear();
  }
}
