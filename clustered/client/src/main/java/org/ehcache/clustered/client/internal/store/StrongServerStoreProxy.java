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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Ludovic Orban
 */
public class StrongServerStoreProxy implements ServerStoreProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(StrongServerStoreProxy.class);

  private final ServerStoreProxy delegate;
  private final ConcurrentMap<Long, CountDownLatch> hashInvalidationsInProgress = new ConcurrentHashMap<Long, CountDownLatch>();
  private final Lock invalidateAllLock = new ReentrantLock();
  private CountDownLatch invalidateAllLatch;
  private final List<InvalidationListener> invalidationListeners = new CopyOnWriteArrayList<InvalidationListener>();
  private final EhcacheClientEntity entity;

  public StrongServerStoreProxy(final ServerStoreMessageFactory messageFactory, final EhcacheClientEntity entity) {
    this.delegate = new NoInvalidationServerStoreProxy(messageFactory, entity);
    this.entity = entity;
    entity.addResponseListener(EhcacheEntityResponse.HashInvalidationDone.class, new EhcacheClientEntity.ResponseListener<EhcacheEntityResponse.HashInvalidationDone>() {
      @Override
      public void onResponse(EhcacheEntityResponse.HashInvalidationDone response) {
        if (response.getCacheId().equals(messageFactory.getCacheId())) {
          long key = response.getKey();
          LOGGER.debug("CLIENT: on cache {}, server notified that clients invalidated hash {}", messageFactory.getCacheId(), key);
          CountDownLatch countDownLatch = hashInvalidationsInProgress.remove(key);
          if (countDownLatch != null) {
            countDownLatch.countDown();
          }
        } else {
          LOGGER.debug("CLIENT: on cache {}, ignoring invalidation on unrelated cache : {}", messageFactory.getCacheId(), response.getCacheId());
        }
      }
    });
    entity.addResponseListener(EhcacheEntityResponse.AllInvalidationDone.class, new EhcacheClientEntity.ResponseListener<EhcacheEntityResponse.AllInvalidationDone>() {
      @Override
      public void onResponse(EhcacheEntityResponse.AllInvalidationDone response) {
        if (response.getCacheId().equals(messageFactory.getCacheId())) {
          LOGGER.debug("CLIENT: on cache {}, server notified that clients invalidated all", messageFactory.getCacheId());

          CountDownLatch countDownLatch;
          invalidateAllLock.lock();
          try {
            countDownLatch = invalidateAllLatch;
            invalidateAllLatch = null;
          } finally {
            invalidateAllLock.unlock();
          }

          if (countDownLatch != null) {
            LOGGER.debug("CLIENT: on cache {}, count down", messageFactory.getCacheId());
            countDownLatch.countDown();
          }
        } else {
          LOGGER.debug("CLIENT: on cache {}, ignoring invalidation on unrelated cache : {}", messageFactory.getCacheId(), response.getCacheId());
        }
      }
    });
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

          try {
            LOGGER.debug("CLIENT: ack'ing invalidation of hash {} from cache {} (ID {})", key, cacheId, invalidationId);
            entity.invokeAsync(messageFactory.clientInvalidationAck(invalidationId), true);
          } catch (Exception e) {
            //TODO: what should be done here?
            LOGGER.error("error acking client invalidation of hash {} on cache {}", key, cacheId, e);
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

          try {
            LOGGER.debug("CLIENT: ack'ing invalidation of all from cache {} (ID {})", cacheId, invalidationId);
            entity.invokeAsync(messageFactory.clientInvalidationAck(invalidationId), true);
          } catch (Exception e) {
            //TODO: what should be done here?
            LOGGER.error("error acking client invalidation of all on cache {}", cacheId, e);
          }
        } else {
          LOGGER.debug("CLIENT: on cache {}, ignoring invalidation on unrelated cache : {}", messageFactory.getCacheId(), response.getCacheId());
        }
      }
    });
    entity.addDisconnectionListener(new EhcacheClientEntity.DisconnectionListener() {
      @Override
      public void onDisconnection() {
        for (Map.Entry<Long, CountDownLatch> entry : hashInvalidationsInProgress.entrySet()) {
          entry.getValue().countDown();
        }
        hashInvalidationsInProgress.clear();

        invalidateAllLock.lock();
        try {
          if (invalidateAllLatch != null) {
            invalidateAllLatch.countDown();
          }
        } finally {
          invalidateAllLock.unlock();
        }
      }
    });
  }

  private <T> T performWaitingForHashInvalidation(long key, NullaryFunction<T> c) throws InterruptedException, TimeoutException {
    CountDownLatch latch = new CountDownLatch(1);
    while (true) {
      if (!entity.isConnected()) {
        throw new IllegalStateException("Clustered tier manager disconnected");
      }
      CountDownLatch countDownLatch = hashInvalidationsInProgress.putIfAbsent(key, latch);
      if (countDownLatch == null) {
        break;
      }
      awaitOnLatch(countDownLatch);
    }

    try {
      T result = c.apply();
      awaitOnLatch(latch);
      LOGGER.debug("CLIENT: key {} invalidated on all clients, unblocking call", key);
      return result;
    } catch (Exception ex) {
      hashInvalidationsInProgress.remove(key);
      latch.countDown();

      if (ex instanceof TimeoutException) {
        throw (TimeoutException)ex;
      }
      throw new RuntimeException(ex);
    }
  }

  private <T> T performWaitingForAllInvalidation(NullaryFunction<T> c) throws InterruptedException, TimeoutException {
    CountDownLatch newLatch = new CountDownLatch(1);
    while (true) {
      if (!entity.isConnected()) {
        throw new IllegalStateException("Clustered tier manager disconnected");
      }

      CountDownLatch existingLatch;
      invalidateAllLock.lock();
      try {
        existingLatch = invalidateAllLatch;
        if (existingLatch == null) {
          invalidateAllLatch = newLatch;
          break;
        }
      } finally {
        invalidateAllLock.unlock();
      }

      awaitOnLatch(existingLatch);
    }

    try {
      T result = c.apply();
      awaitOnLatch(newLatch);
      LOGGER.debug("CLIENT: all invalidated on all clients, unblocking call");
      return result;
    } catch (Exception ex) {
      invalidateAllLock.lock();
      try {
        invalidateAllLatch = null;
      } finally {
        invalidateAllLock.unlock();
      }
      newLatch.countDown();

      if (ex instanceof TimeoutException) {
        throw (TimeoutException)ex;
      }
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
      throw new IllegalStateException("Clustered tier manager disconnected");
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
  public boolean removeInvalidationListener(InvalidationListener listener) {
    return invalidationListeners.remove(listener);
  }

  @Override
  public Chain get(long key) throws TimeoutException {
    return delegate.get(key);
  }

  @Override
  public void append(final long key, final ByteBuffer payLoad) throws TimeoutException {
    try {
      performWaitingForHashInvalidation(key, new NullaryFunction<Void>() {
        @Override
        public Void apply() throws TimeoutException {
          delegate.append(key, payLoad);
          return null;
        }
      });
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public Chain getAndAppend(final long key, final ByteBuffer payLoad) throws TimeoutException {
    try {
      return performWaitingForHashInvalidation(key, new NullaryFunction<Chain>() {
        @Override
        public Chain apply() throws TimeoutException {
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
  public void clear() throws TimeoutException {
    try {
      performWaitingForAllInvalidation(new NullaryFunction<Object>() {
        @Override
        public Object apply() throws TimeoutException {
          delegate.clear();
          return null;
        }
      });
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  private interface NullaryFunction<T> {
    T apply() throws Exception;
  }
}
