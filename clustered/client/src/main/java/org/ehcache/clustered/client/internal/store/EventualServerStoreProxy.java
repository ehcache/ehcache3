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
import org.ehcache.clustered.common.store.Chain;

import java.nio.ByteBuffer;

/**
 * @author Ludovic Orban
 */
public class EventualServerStoreProxy implements ServerStoreProxy {

  private final ServerStoreProxy delegate;

  public EventualServerStoreProxy(String cacheId, final EhcacheClientEntity entity) {
    this.delegate = new NoInvalidationServerStoreProxy(cacheId, entity);
    entity.addResponseListener(EhcacheEntityResponse.ClientInvalidateHash.class, new EhcacheClientEntity.ResponseListener<EhcacheEntityResponse.ClientInvalidateHash>() {
      @Override
      public void onResponse(EhcacheEntityResponse.ClientInvalidateHash response) {
        final String cacheId = response.getCacheId();
        final long key = response.getKey();
        final int invalidationId = response.getInvalidationId();

        System.out.println("CLIENT: doing work to invalidate hash " + key + " from cache " + cacheId + " (ID " + invalidationId + ")");
        //TODO: wire invalidation valve here
      }
    });
  }

  @Override
  public String getCacheId() {
    return delegate.getCacheId();
  }

  @Override
  public Chain get(long key) {
    return delegate.get(key);
  }

  @Override
  public void append(final long key, final ByteBuffer payLoad) {
    delegate.append(key, payLoad);
  }

  @Override
  public Chain getAndAppend(final long key, final ByteBuffer payLoad) {
    return delegate.getAndAppend(key, payLoad);
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    delegate.replaceAtHead(key, expect, update);
  }
}
