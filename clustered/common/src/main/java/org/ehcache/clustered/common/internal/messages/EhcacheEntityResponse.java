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

  public static HashInvalidationDone hashInvalidationDone(String cacheId, long key) {
    return new HashInvalidationDone(cacheId, key);
  }

  public static class HashInvalidationDone extends EhcacheEntityResponse {
    private final String cacheId;
    private final long key;

    HashInvalidationDone(String cacheId, long key) {
      this.cacheId = cacheId;
      this.key = key;
    }

    public String getCacheId() {
      return cacheId;
    }

    public long getKey() {
      return key;
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.HASH_INVALIDATION_DONE;
    }
  }

  public static AllInvalidationDone allInvalidationDone(String cacheId) {
    return new AllInvalidationDone(cacheId);
  }

  public static class AllInvalidationDone extends EhcacheEntityResponse {
    private final String cacheId;

    AllInvalidationDone(String cacheId) {
      this.cacheId = cacheId;
    }

    public String getCacheId() {
      return cacheId;
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.ALL_INVALIDATION_DONE;
    }
  }

  public static ServerInvalidateHash serverInvalidateHash(String cacheId, long key) {
    return new ServerInvalidateHash(cacheId, key);
  }

  public static class ServerInvalidateHash extends EhcacheEntityResponse {
    private final String cacheId;
    private final long key;

    public ServerInvalidateHash(String cacheId, long key) {
      this.cacheId = cacheId;
      this.key = key;
    }

    public String getCacheId() {
      return cacheId;
    }

    public long getKey() {
      return key;
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.SERVER_INVALIDATE_HASH;
    }
  }

  public static ClientInvalidateHash clientInvalidateHash(String cacheId, long key, int invalidationId) {
    return new ClientInvalidateHash(cacheId, key, invalidationId);
  }

  public static class ClientInvalidateHash extends EhcacheEntityResponse {
    private final String cacheId;
    private final long key;
    private final int invalidationId;

    public ClientInvalidateHash(String cacheId, long key, int invalidationId) {
      this.cacheId = cacheId;
      this.key = key;
      this.invalidationId = invalidationId;
    }

    public String getCacheId() {
      return cacheId;
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

  public static ClientInvalidateAll clientInvalidateAll(String cacheId, int invalidationId) {
    return new ClientInvalidateAll(cacheId, invalidationId);
  }

  public static class ClientInvalidateAll extends EhcacheEntityResponse {
    private final String cacheId;
    private final int invalidationId;

    public ClientInvalidateAll(String cacheId, int invalidationId) {
      this.cacheId = cacheId;
      this.invalidationId = invalidationId;
    }

    public String getCacheId() {
      return cacheId;
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

}
