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

import org.ehcache.clustered.common.internal.messages.ClusterTierReconnectMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.store.Chain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.ehcache.clustered.client.config.Timeouts.nanosStartingFromNow;

public class StrongServerStoreProxy implements ServerStoreProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(StrongServerStoreProxy.class);

  private final CommonServerStoreProxy delegate;
  private final ConcurrentMap<Long, CountDownLatch> hashInvalidationsInProgress = new ConcurrentHashMap<>();
  private final AtomicReference<CountDownLatch> invalidateAllLatch = new AtomicReference<>();
  private final ClusterTierClientEntity entity;

  public StrongServerStoreProxy(final String cacheId, final ClusterTierClientEntity entity, final ServerCallback invalidation) {
    this.delegate = new CommonServerStoreProxy(cacheId, entity, invalidation);
    this.entity = entity;
    delegate.addResponseListener(EhcacheEntityResponse.HashInvalidationDone.class, this::hashInvalidationDoneResponseListener);
    delegate.addResponseListener(EhcacheEntityResponse.AllInvalidationDone.class, this::allInvalidationDoneResponseListener);

    entity.addReconnectListener(this::reconnectListener);
    entity.addDisconnectionListener(this::disconnectionListener);
  }

  private void disconnectionListener() {
    for (CountDownLatch latch : hashInvalidationsInProgress.values()) {
      latch.countDown();
    }
    hashInvalidationsInProgress.clear();

    CountDownLatch countDownLatch = invalidateAllLatch.get();
    if (countDownLatch != null) {
      countDownLatch.countDown();
    }
  }

  private void allInvalidationDoneResponseListener(EhcacheEntityResponse.AllInvalidationDone response) {
    LOGGER.debug("CLIENT: on cache {}, server notified that clients invalidated all", getCacheId());

    CountDownLatch countDownLatch = invalidateAllLatch.getAndSet(null);
    if (countDownLatch != null) {
      LOGGER.debug("CLIENT: on cache {}, count down", getCacheId());
      countDownLatch.countDown();
    }
  }

  private void hashInvalidationDoneResponseListener(EhcacheEntityResponse.HashInvalidationDone response) {
    long key = response.getKey();
    LOGGER.debug("CLIENT: on cache {}, server notified that clients invalidated hash {}", getCacheId(), key);
    CountDownLatch countDownLatch = hashInvalidationsInProgress.remove(key);
    if (countDownLatch != null) {
      countDownLatch.countDown();
    }
  }

  private void reconnectListener(ClusterTierReconnectMessage reconnectMessage) {
    Set<Long> inflightInvalidations = hashInvalidationsInProgress.keySet();
    reconnectMessage.addInvalidationsInProgress(inflightInvalidations);
    if (invalidateAllLatch.get() != null) {
      reconnectMessage.clearInProgress();
    }
  }

  private <T> T performWaitingForHashInvalidation(long key, Callable<T> c, Duration timeout) throws TimeoutException {
    LongSupplier nanosRemaining = nanosStartingFromNow(timeout);

    CountDownLatch latch = new CountDownLatch(1);
    while (true) {
      if (!entity.isConnected()) {
        throw new IllegalStateException("Cluster tier manager disconnected");
      }
      CountDownLatch countDownLatch = hashInvalidationsInProgress.putIfAbsent(key, latch);
      if (countDownLatch == null) {
        break;
      }
      awaitOnLatch(countDownLatch, nanosRemaining);
    }

    try {
      T result = c.call();
      LOGGER.debug("CLIENT: Waiting for invalidations on key {}", key);
      awaitOnLatch(latch, nanosRemaining);
      LOGGER.debug("CLIENT: key {} invalidated on all clients, unblocking call", key);
      return result;
    } catch (Exception ex) {
      hashInvalidationsInProgress.remove(key);
      latch.countDown();

      if (ex instanceof TimeoutException) {
        throw (TimeoutException)ex;
      }
      if (ex instanceof ServerStoreProxyException) {
        throw (ServerStoreProxyException)ex;
      }
      throw new RuntimeException(ex);
    }
  }

  private <T> T performWaitingForAllInvalidation(Callable<T> c, Duration timeout) throws TimeoutException {
    LongSupplier nanosRemaining = nanosStartingFromNow(timeout);

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
          awaitOnLatch(existingLatch, nanosRemaining);
        }
      }
    }

    try {
      T result = c.call();
      awaitOnLatch(newLatch, nanosRemaining);
      LOGGER.debug("CLIENT: all invalidated on all clients, unblocking call");
      return result;
    } catch (Exception ex) {
      invalidateAllLatch.set(null);
      newLatch.countDown();

      if (ex instanceof TimeoutException) {
        throw (TimeoutException)ex;
      }
      if (ex instanceof ServerStoreProxyException) {
        throw (ServerStoreProxyException)ex;
      }
      throw new RuntimeException(ex);
    }
  }

  private void awaitOnLatch(CountDownLatch countDownLatch, LongSupplier nanosRemaining) throws TimeoutException {
    boolean interrupted = Thread.interrupted();
    try {
      while (true) {
        try {
          if (countDownLatch.await(nanosRemaining.getAsLong(), TimeUnit.NANOSECONDS)) {
            if (!entity.isConnected()) {
              throw new IllegalStateException("Cluster tier manager disconnected");
            } else {
              return;
            }
          } else {
            throw new TimeoutException();
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
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
    performWaitingForHashInvalidation(key, () -> {
      delegate.append(key, payLoad);
      return null;
    }, entity.getTimeouts().getWriteOperationTimeout());
  }

  @Override
  public ChainEntry getAndAppend(final long key, final ByteBuffer payLoad) throws TimeoutException {
    return performWaitingForHashInvalidation(key, () -> delegate.getAndAppend(key, payLoad), entity.getTimeouts().getWriteOperationTimeout());
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    delegate.replaceAtHead(key, expect, update);
  }

  @Override
  public void clear() throws TimeoutException {
    performWaitingForAllInvalidation(() -> {
      delegate.clear();
      return null;
    }, entity.getTimeouts().getWriteOperationTimeout());
  }

  @Override
  public Iterator<Chain> iterator() throws TimeoutException {
    return delegate.iterator();
  }
}
