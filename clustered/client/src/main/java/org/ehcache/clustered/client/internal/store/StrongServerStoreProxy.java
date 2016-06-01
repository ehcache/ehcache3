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
import org.ehcache.clustered.common.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.messages.ServerStoreMessageFactory;
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.core.spi.function.NullaryFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Ludovic Orban
 */
public class StrongServerStoreProxy implements ServerStoreProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(StrongServerStoreProxy.class);

  private final ServerStoreProxy delegate;
  private final ConcurrentMap<Long, CountDownLatch> invalidationsInProgress = new ConcurrentHashMap<Long, CountDownLatch>();
  private final List<InvalidationListener> invalidationListeners = new CopyOnWriteArrayList<InvalidationListener>();
  private final EhcacheClientEntity entity;

  public StrongServerStoreProxy(final ServerStoreMessageFactory messageFactory, final EhcacheClientEntity entity) {
    this.delegate = new NoInvalidationServerStoreProxy(messageFactory, entity);
    this.entity = entity;
    entity.addResponseListener(EhcacheEntityResponse.InvalidationDone.class, new EhcacheClientEntity.ResponseListener<EhcacheEntityResponse.InvalidationDone>() {
      @Override
      public void onResponse(EhcacheEntityResponse.InvalidationDone response) {
        if (response.getCacheId().equals(messageFactory.getCacheId())) {
          long key = response.getKey();
          LOGGER.debug("CLIENT: on cache {}, server notified that clients invalidated key {}", messageFactory.getCacheId(), key);
          CountDownLatch countDownLatch = invalidationsInProgress.remove(key);
          if (countDownLatch != null) {
            countDownLatch.countDown();
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

        LOGGER.debug("CLIENT: doing work to invalidate hash {} from cache {} (ID {})", key, cacheId, invalidationId);
        for (InvalidationListener listener : invalidationListeners) {
          listener.onInvalidationRequest(key);
        }

        try {
          LOGGER.debug("CLIENT: ack'ing invalidation of hash {} from cache {} (ID {})", key, cacheId, invalidationId);
          entity.invoke(messageFactory.clientInvalidateHashAck(key, invalidationId), true);
        } catch (Exception e) {
          //TODO: what should be done here?
          LOGGER.error("error acking client invalidation of hash {} on cache {}", key, cacheId, e);
        }
      }
    });
    entity.addDisconnectionListener(new EhcacheClientEntity.DisconnectionListener() {
      @Override
      public void onDisconnection() {
        for (Map.Entry<Long, CountDownLatch> entry : invalidationsInProgress.entrySet()) {
          entry.getValue().countDown();
        }
        invalidationsInProgress.clear();
      }
    });
  }

  private <T> T performWaitingForInvalidation(long key, NullaryFunction<T> c) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    while (true) {
      if (!entity.isConnected()) {
        throw new IllegalStateException("Entity disconnected");
      }
      CountDownLatch countDownLatch = invalidationsInProgress.putIfAbsent(key, latch);
      if (countDownLatch == null) {
        break;
      }
      awaitOnLatch(countDownLatch);
    }

    try {
      T result = c.apply();
      awaitOnLatch(latch);
      LOGGER.debug("CLIENT: key {} invalidated on all clients, unblocking append", key);
      return result;
    } catch (Exception ex) {
      invalidationsInProgress.remove(key);
      latch.countDown();
      throw new RuntimeException(ex);
    }
  }

  private void awaitOnLatch(CountDownLatch countDownLatch) throws InterruptedException {
    int totalAwaitTime = 0;
    int backoff = 1;
    while (!countDownLatch.await(backoff, TimeUnit.SECONDS)) {
      totalAwaitTime += backoff;
      backoff = (backoff >= 10) ? 10 : backoff * 2;
      LOGGER.debug("Waiting for the server's InvalidationDone message for {}s, backing off {}s...", totalAwaitTime, backoff);
    }
    if (!entity.isConnected()) {
      throw new IllegalStateException("Entity disconnected");
    }
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
  public Chain get(long key) {
    return delegate.get(key);
  }

  @Override
  public void append(final long key, final ByteBuffer payLoad) {
    try {
      performWaitingForInvalidation(key, new NullaryFunction<Void>() {
        @Override
        public Void apply() {
          delegate.append(key, payLoad);
          return null;
        }
      });
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public Chain getAndAppend(final long key, final ByteBuffer payLoad) {
    try {
      return performWaitingForInvalidation(key, new NullaryFunction<Chain>() {
        @Override
        public Chain apply() {
          return delegate.getAndAppend(key, payLoad);
        }
      });
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    delegate.replaceAtHead(key, expect, update);
  }

  @Override
  public void clear() {
    delegate.clear();
  }
}
