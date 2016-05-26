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
import org.ehcache.core.spi.function.NullaryFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * @author Ludovic Orban
 */
public class StrongServerStoreProxy implements ServerStoreProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(StrongServerStoreProxy.class);

  private final ServerStoreProxy delegate;
  private final ConcurrentMap<Long, CountDownLatch> invalidationsInProgress = new ConcurrentHashMap<Long, CountDownLatch>();
  private final List<InvalidationListener> invalidationListeners = new CopyOnWriteArrayList<InvalidationListener>();

  public StrongServerStoreProxy(String cacheId, final EhcacheClientEntity entity) {
    this.delegate = new NoInvalidationServerStoreProxy(cacheId, entity);
    entity.addResponseListener(EhcacheEntityResponse.InvalidationDone.class, new EhcacheClientEntity.ResponseListener<EhcacheEntityResponse.InvalidationDone>() {
      @Override
      public void onResponse(EhcacheEntityResponse.InvalidationDone response) {
        if (response.getCacheId().equals(getCacheId())) {
          long key = response.getKey();
          System.out.println("CLIENT: on cache " + getCacheId() + ", server notified that clients invalidated key " + key);
          CountDownLatch countDownLatch = invalidationsInProgress.remove(key);
          countDownLatch.countDown();
        } else {
          System.out.println("CLIENT: on cache " + getCacheId() + ", ignoring invalidation on unrelated cache : " + response.getCacheId());
        }
      }
    });
    entity.addResponseListener(EhcacheEntityResponse.ClientInvalidateHash.class, new EhcacheClientEntity.ResponseListener<EhcacheEntityResponse.ClientInvalidateHash>() {
      @Override
      public void onResponse(EhcacheEntityResponse.ClientInvalidateHash response) {
        final String cacheId = response.getCacheId();
        final long key = response.getKey();
        final int invalidationId = response.getInvalidationId();

        System.out.println("CLIENT: doing work to invalidate hash " + key + " from cache " + cacheId + " (ID " + invalidationId + ")");
        for (InvalidationListener listener : invalidationListeners) {
          listener.onInvalidationRequest(key);
        }

        try {
          System.out.println("CLIENT: ack'ing invalidation of hash " + key + " from cache " + cacheId + " (ID " + invalidationId + ")");
          entity.invoke(EhcacheEntityMessage.clientInvalidateHashAck(cacheId, key, invalidationId), true); //TODO: wait until replicated or not?
        } catch (Exception e) {
          LOGGER.warn("error acking client invalidation of hash " + key + " on cache " + cacheId, e);
        }
      }
    });
  }

  private <T> T performWaitingForInvalidationIfNeeded(long key, NullaryFunction<T> c) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    while (true) {
      CountDownLatch countDownLatch = invalidationsInProgress.putIfAbsent(key, latch);
      if (countDownLatch == null) {
        break;
      }
      countDownLatch.await();
    }

    try {
      return c.apply();
    } finally {
      latch.await();
      System.out.println("CLIENT: key " + key + " invalidated on all clients, unblocking append");
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
      performWaitingForInvalidationIfNeeded(key, new NullaryFunction<Void>() {
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
      return performWaitingForInvalidationIfNeeded(key, new NullaryFunction<Chain>() {
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
}
