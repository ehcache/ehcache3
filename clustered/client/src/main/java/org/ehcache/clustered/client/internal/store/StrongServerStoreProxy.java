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

import org.ehcache.clustered.client.config.TimeoutDuration;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity.ReconnectListener;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity.ResponseListener;
import org.ehcache.clustered.common.internal.messages.ClusterTierReconnectMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.AllInvalidationDone;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.HashInvalidationDone;
import org.ehcache.clustered.common.internal.messages.ServerStoreMessageFactory;
import org.ehcache.clustered.common.internal.store.Chain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class StrongServerStoreProxy implements ServerStoreProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(StrongServerStoreProxy.class);

  private final CommonServerStoreProxy delegate;
  private final ConcurrentMap<Long, CountDownLatch> hashInvalidationsInProgress = new ConcurrentHashMap<Long, CountDownLatch>();
  private final AtomicReference<CountDownLatch> invalidateAllLatch = new AtomicReference<CountDownLatch>();
  private final ClusterTierClientEntity entity;

  public StrongServerStoreProxy(final String cacheId, final ServerStoreMessageFactory messageFactory, final ClusterTierClientEntity entity) {
    this.delegate = new CommonServerStoreProxy(cacheId, messageFactory, entity);
    this.entity = entity;
    entity.setReconnectListener(new ReconnectListener() {
      @Override
      public void onHandleReconnect(ClusterTierReconnectMessage reconnectMessage) {
        Set<Long> inflightInvalidations = hashInvalidationsInProgress.keySet();
        reconnectMessage.addInvalidationsInProgress(inflightInvalidations);
        if (invalidateAllLatch.get() != null) {
          reconnectMessage.clearInProgress();
        }
      }
    });

    delegate.addResponseListeners(HashInvalidationDone.class, new ResponseListener<HashInvalidationDone>() {
      @Override
      public void onResponse(HashInvalidationDone response) {
        long key = response.getKey();
        LOGGER.debug("CLIENT: on cache {}, server notified that clients invalidated hash {}", cacheId, key);
        CountDownLatch countDownLatch = hashInvalidationsInProgress.remove(key);
        if (countDownLatch != null) {
          countDownLatch.countDown();
        }
      }
    });
    delegate.addResponseListeners(AllInvalidationDone.class, new ResponseListener<AllInvalidationDone>() {
      @Override
      public void onResponse(AllInvalidationDone response) {
        LOGGER.debug("CLIENT: on cache {}, server notified that clients invalidated all", cacheId);

        CountDownLatch countDownLatch = invalidateAllLatch.getAndSet(null);
        if (countDownLatch != null) {
          LOGGER.debug("CLIENT: on cache {}, count down", cacheId);
          countDownLatch.countDown();
        }
      }
    });

    entity.setDisconnectionListener(new SimpleClusterTierClientEntity.DisconnectionListener() {
      @Override
      public void onDisconnection() {
        for (CountDownLatch latch : hashInvalidationsInProgress.values()) {
          latch.countDown();
        }
        hashInvalidationsInProgress.clear();

        CountDownLatch countDownLatch = invalidateAllLatch.get();
        if (countDownLatch != null) {
          countDownLatch.countDown();
        }
      }
    });
  }

  private <T> T performWaitingForHashInvalidation(long key, Callable<T> c, long timeoutNanos) throws InterruptedException, TimeoutException {
    final long start = System.nanoTime();

    CountDownLatch latch = new CountDownLatch(1);
    while (true) {
      if (!entity.isConnected()) {
        throw new IllegalStateException("Cluster tier manager disconnected");
      }
      CountDownLatch countDownLatch = hashInvalidationsInProgress.putIfAbsent(key, latch);
      if (countDownLatch == null) {
        break;
      }
      awaitOnLatch(countDownLatch, timeoutNanos - (System.nanoTime() - start));
    }

    try {
      T result = c.call();
      LOGGER.debug("CLIENT: Waiting for invalidations on key {}", key);
      awaitOnLatch(latch, timeoutNanos - (System.nanoTime() - start));
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

  private <T> T performWaitingForAllInvalidation(Callable<T> c, long timeoutNanos) throws InterruptedException, TimeoutException {
    final long start = System.nanoTime();

    CountDownLatch newLatch = new CountDownLatch(1);
    while (true) {
      if (!entity.isConnected()) {
        throw new IllegalStateException("Cluster tier manager disconnected");
      }

      if (invalidateAllLatch.compareAndSet(null, newLatch)) {
        break;
      } else {
        CountDownLatch existingLatch = invalidateAllLatch.get();
        if (existingLatch != null) {
          awaitOnLatch(existingLatch, timeoutNanos - (System.nanoTime() - start));
        }
      }
    }

    try {
      T result = c.call();
      awaitOnLatch(newLatch, timeoutNanos - (System.nanoTime() - start));
      LOGGER.debug("CLIENT: all invalidated on all clients, unblocking call");
      return result;
    } catch (Exception ex) {
      invalidateAllLatch.set(null);
      newLatch.countDown();

      if (ex instanceof TimeoutException) {
        throw (TimeoutException)ex;
      }
      throw new RuntimeException(ex);
    }
  }

  private void awaitOnLatch(CountDownLatch countDownLatch, long timeoutNanos) throws InterruptedException, TimeoutException {
    if (countDownLatch.await(timeoutNanos, TimeUnit.NANOSECONDS)) {
      if (!entity.isConnected()) {
        throw new IllegalStateException("Cluster tier manager disconnected");
      } else {
        return;
      }
    } else {
      throw new TimeoutException();
    }
  }


  @Override
  public String getCacheId() {
    return delegate.getCacheId();
  }

  @Override
  public void addInvalidationListener(InvalidationListener listener) {
    delegate.addInvalidationListener(listener);
  }

  @Override
  public boolean removeInvalidationListener(InvalidationListener listener) {
    return delegate.removeInvalidationListener(listener);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public Chain get(long key) throws TimeoutException {
    return delegate.get(key);
  }

  @Override
  public void append(final long key, final ByteBuffer payLoad) throws TimeoutException {
    try {
      performWaitingForHashInvalidation(key, new Callable<Void>() {
        @Override
        public Void call() throws TimeoutException {
          delegate.append(key, payLoad);
          return null;
        }
      }, entity.getTimeouts().getMutativeOperationTimeout().toNanos());
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public Chain getAndAppend(final long key, final ByteBuffer payLoad) throws TimeoutException {
    try {
      return performWaitingForHashInvalidation(key, new Callable<Chain>() {
        @Override
        public Chain call() throws TimeoutException {
          return delegate.getAndAppend(key, payLoad);
        }
      }, entity.getTimeouts().getMutativeOperationTimeout().toNanos());
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
      performWaitingForAllInvalidation(new Callable<Object>() {
        @Override
        public Object call() throws TimeoutException {
          delegate.clear();
          return null;
        }
      }, entity.getTimeouts().getMutativeOperationTimeout().toNanos());
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }
}
