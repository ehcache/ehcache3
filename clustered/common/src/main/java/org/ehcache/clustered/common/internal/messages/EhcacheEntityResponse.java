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

package org.ehcache.clustered.common.internal.messages;


import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.store.Chain;
import org.terracotta.entity.EntityResponse;

import java.util.Set;

public abstract class EhcacheEntityResponse implements EntityResponse {

  public abstract EhcacheResponseType getResponseType();

  public static class Success extends EhcacheEntityResponse {

    public static final Success INSTANCE = new Success();

    private Success() {
      //singleton
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.SUCCESS;
    }
  }

  public static class Failure extends EhcacheEntityResponse {

    private final ClusterException cause;

    Failure(ClusterException cause) {
      this.cause = cause;
    }

    public ClusterException getCause() {
      return cause;
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.FAILURE;
    }
  }

  public static class GetResponse extends EhcacheEntityResponse {

    private final Chain chain;

    GetResponse(Chain chain) {
      this.chain = chain;
    }

    public Chain getChain() {
      return chain;
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.GET_RESPONSE;
    }
  }

  public static HashInvalidationDone hashInvalidationDone(long key) {
    return new HashInvalidationDone(key);
  }

  public static class HashInvalidationDone extends EhcacheEntityResponse {
    private final long key;

    HashInvalidationDone(long key) {
      this.key = key;
    }

    public long getKey() {
      return key;
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.HASH_INVALIDATION_DONE;
    }
  }

  public static AllInvalidationDone allInvalidationDone() {
    return new AllInvalidationDone();
  }

  public static class AllInvalidationDone extends EhcacheEntityResponse {

    AllInvalidationDone() {
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.ALL_INVALIDATION_DONE;
    }
  }

  public static ServerInvalidateHash serverInvalidateHash(long key) {
    return new ServerInvalidateHash(key);
  }

  public static class ServerInvalidateHash extends EhcacheEntityResponse {
    private final long key;

    public ServerInvalidateHash(long key) {
      this.key = key;
    }

    public long getKey() {
      return key;
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.SERVER_INVALIDATE_HASH;
    }
  }

  public static ClientInvalidateHash clientInvalidateHash(long key, int invalidationId) {
    return new ClientInvalidateHash(key, invalidationId);
  }

  public static class ClientInvalidateHash extends EhcacheEntityResponse {
    private final long key;
    private final int invalidationId;

    public ClientInvalidateHash(long key, int invalidationId) {
      this.key = key;
      this.invalidationId = invalidationId;
    }

    public long getKey() {
      return key;
    }

    public int getInvalidationId() {
      return invalidationId;
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.CLIENT_INVALIDATE_HASH;
    }
  }

  public static ClientInvalidateAll clientInvalidateAll(int invalidationId) {
    return new ClientInvalidateAll(invalidationId);
  }

  public static class ClientInvalidateAll extends EhcacheEntityResponse {
    private final int invalidationId;

    public ClientInvalidateAll(int invalidationId) {
      this.invalidationId = invalidationId;
    }

    public int getInvalidationId() {
      return invalidationId;
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.CLIENT_INVALIDATE_ALL;
    }
  }

  public static MapValue mapValue(Object value) {
    return new MapValue(value);
  }

  public static class MapValue extends EhcacheEntityResponse {

    private final Object value;

    public MapValue(Object value) {
      this.value = value;
    }

    public Object getValue() {
      return this.value;
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.MAP_VALUE;
    }
  }

  public static class PrepareForDestroy extends EhcacheEntityResponse {

    private final Set<String> stores;

    public PrepareForDestroy(Set<String> stores) {
      this.stores = stores;
    }

    @Override
    public EhcacheResponseType getResponseType() {
      return EhcacheResponseType.PREPARE_FOR_DESTROY;
    }

    public Set<String> getStores() {
      return stores;
    }
  }

}
