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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Provides client-side access to the services of a {@code ServerStore}.
 */
public class ServerStoreProxy implements ServerStore {

  private final String cacheId;
  private final EhcacheClientEntity entity;
  private final ConcurrentMap<Long, List<CountDownLatch>> invalidationsInProgress = new ConcurrentHashMap<Long, List<CountDownLatch>>();

  public ServerStoreProxy(String cacheId, EhcacheClientEntity entity) {
    this.cacheId = cacheId;
    this.entity = entity;
    entity.addResponseListener(EhcacheEntityResponse.InvalidationDone.class, new EhcacheClientEntity.ResponseListener<EhcacheEntityResponse.InvalidationDone>() {
      @Override
      public void onResponse(EhcacheEntityResponse.InvalidationDone response) {
        if (response.getCacheId().equals(ServerStoreProxy.this.cacheId)) {
          long key = response.getKey();
          System.out.println("CLIENT: on cache " + ServerStoreProxy.this.cacheId + ", server notified that clients invalidated key " + key);
          List<CountDownLatch> countDownLatches = invalidationsInProgress.get(key);
          countDownLatches.remove(0).countDown();
          if (countDownLatches.isEmpty()) {
            invalidationsInProgress.remove(key, Collections.emptyList());
          }
        } else {
          System.out.println("CLIENT: on cache " + ServerStoreProxy.this.cacheId + ", ignoring invalidation on unrelated cache : " + response.getCacheId());
        }
      }
    });
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
  public void append(long key, ByteBuffer payLoad) {
    try {
      List<CountDownLatch> countDownLatches = invalidationsInProgress.get(key);
      if (countDownLatches == null) {
        countDownLatches = new CopyOnWriteArrayList<CountDownLatch>();
        List<CountDownLatch> raceWinner = invalidationsInProgress.putIfAbsent(key, countDownLatches);
        if (raceWinner != null) {
          countDownLatches = raceWinner;
        }
      }
      CountDownLatch latch = new CountDownLatch(1);
      countDownLatches.add(latch);

      entity.invoke(EhcacheEntityMessage.appendOperation(cacheId, key, payLoad), true);

      //TODO: eventual should not need to wait on a latch
      latch.await();
      System.out.println("CLIENT: key " + key + " invalidated on all clients, unblocking append");
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
  }

  @Override
  public Chain getAndAppend(long key, ByteBuffer payLoad) {
    EhcacheEntityResponse response;
    try {
      response = entity.invoke(EhcacheEntityMessage.getAndAppendOperation(cacheId, key, payLoad), true);
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
