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
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.ServerStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

/**
 * Provides client-side access to the services of a {@code ServerStore}.
 */
public class ServerStoreProxy implements ServerStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerStoreProxy.class);

  private final String cacheId;
  private final EhcacheClientEntity entity;
  private final Consistency consistency;
  private final ConcurrentMap<Long, CountDownLatch> invalidationsInProgress = new ConcurrentHashMap<Long, CountDownLatch>();

  public ServerStoreProxy(String cacheId, final EhcacheClientEntity entity, final Consistency consistency) {
    this.cacheId = cacheId;
    this.entity = entity;
    this.consistency = consistency;
    if (consistency == Consistency.STRONG) {
      entity.addResponseListener(EhcacheEntityResponse.InvalidationDone.class, new EhcacheClientEntity.ResponseListener<EhcacheEntityResponse.InvalidationDone>() {
        @Override
        public void onResponse(EhcacheEntityResponse.InvalidationDone response) {
          if (response.getCacheId().equals(ServerStoreProxy.this.cacheId)) {
            long key = response.getKey();
            System.out.println("CLIENT: on cache " + ServerStoreProxy.this.cacheId + ", server notified that clients invalidated key " + key);
            CountDownLatch countDownLatch = invalidationsInProgress.remove(key);
            countDownLatch.countDown();
          } else {
            System.out.println("CLIENT: on cache " + ServerStoreProxy.this.cacheId + ", ignoring invalidation on unrelated cache : " + response.getCacheId());
          }
        }
      });
    }
    entity.addResponseListener(EhcacheEntityResponse.ClientInvalidateHash.class, new EhcacheClientEntity.ResponseListener<EhcacheEntityResponse.ClientInvalidateHash>() {
      @Override
      public void onResponse(EhcacheEntityResponse.ClientInvalidateHash response) {
        final String cacheId = response.getCacheId();
        final long key = response.getKey();
        final int invalidationId = response.getInvalidationId();

        System.out.println("CLIENT: doing work to invalidate hash " + key + " from cache " + cacheId + " (ID " + invalidationId + ")");
        //TODO: wire invalidation valve here

        if (consistency == Consistency.STRONG) {
          try {
            System.out.println("CLIENT: ack'ing invalidation of hash " + key + " from cache " + cacheId + " (ID " + invalidationId + ")");
            entity.invoke(EhcacheEntityMessage.clientInvalidateHashAck(cacheId, key, invalidationId), true); //TODO: wait until replicated or not?
          } catch (Exception e) {
            LOGGER.warn("error acking client invalidation of hash " + key + " on cache " + cacheId, e);
          }
        }
      }
    });
  }

  private EhcacheEntityResponse performWaitingForInvalidationIfNeeded(long key, Callable<EhcacheEntityResponse> c) throws Exception {
    CountDownLatch latch = null;
    if (consistency == Consistency.STRONG) {
      latch = new CountDownLatch(1);
      while (true) {
        CountDownLatch countDownLatch = invalidationsInProgress.putIfAbsent(key, latch);
        if (countDownLatch == null) {
          break;
        }
        countDownLatch.await();
      }
    }

    try {
      return c.call();
    } finally {
      if (latch != null) {
        latch.await();
        System.out.println("CLIENT: key " + key + " invalidated on all clients, unblocking append");
      }
    }
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
    EhcacheEntityResponse response;
    try {
      response = entity.invoke(EhcacheEntityMessage.getOperation(cacheId, key), false);
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
    if (response != null && response.getType() == EhcacheEntityResponse.Type.GET_RESPONSE) {
      return ((EhcacheEntityResponse.GetResponse)response).getChain();
    } else {
      throw new ServerStoreProxyException("Response for get operation was invalid : " +
                                          response != null ? response.getType().toString() : "null message");
    }
  }

  @Override
  public void append(final long key, final ByteBuffer payLoad) {
    try {
      performWaitingForInvalidationIfNeeded(key, new Callable<EhcacheEntityResponse>() {
        @Override
        public EhcacheEntityResponse call() throws Exception {
          return entity.invoke(EhcacheEntityMessage.appendOperation(cacheId, key, payLoad), true);
        }
      });
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
  }

  @Override
  public Chain getAndAppend(final long key, final ByteBuffer payLoad) {
    EhcacheEntityResponse response;
    try {
      response = performWaitingForInvalidationIfNeeded(key, new Callable<EhcacheEntityResponse>() {
        @Override
        public EhcacheEntityResponse call() throws Exception {
          return entity.invoke(EhcacheEntityMessage.getAndAppendOperation(cacheId, key, payLoad), true);
        }
      });
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
    if (response != null && response.getType() == EhcacheEntityResponse.Type.GET_RESPONSE) {
      return ((EhcacheEntityResponse.GetResponse)response).getChain();
    } else {
      throw new ServerStoreProxyException("Response for getAndAppend operation was invalid : " +
                                          response != null ? response.getType().toString() : "null message");
    }
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    // TODO: Optimize this method to just send sequences for expect Chain
    try {
      entity.invoke(EhcacheEntityMessage.replaceAtHeadOperation(cacheId, key, expect, update), true);
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
  }
}
