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

  public enum Type {
    SUCCESS((byte) 0),
    FAILURE((byte) 1),
    GET_RESPONSE((byte) 2),
    HASH_INVALIDATION_DONE((byte) 3),
    ALL_INVALIDATION_DONE((byte) 4),
    CLIENT_INVALIDATE_HASH((byte) 5),
    CLIENT_INVALIDATE_ALL((byte) 6),
    SERVER_INVALIDATE_HASH((byte) 7),
    MAP_VALUE((byte) 8),
    ;

    private final byte opCode;

    Type(byte opCode) {
      this.opCode = opCode;
    }

    public byte getOpCode() {
      return this.opCode;
    }

    public static Type responseType(byte opCode) {
      switch (opCode) {
        case 0:
          return SUCCESS;
        case 1:
          return FAILURE;
        case 2:
          return GET_RESPONSE;
        case 3:
          return HASH_INVALIDATION_DONE;
        case 4:
          return ALL_INVALIDATION_DONE;
        case 5:
          return CLIENT_INVALIDATE_HASH;
        case 6:
          return CLIENT_INVALIDATE_ALL;
        case 7:
          return SERVER_INVALIDATE_HASH;
        case 8:
          return MAP_VALUE;
        default:
          throw new IllegalArgumentException("Store operation not defined for : " + opCode);
      }
    }
  }

  public abstract Type getType();

  public static class Success extends EhcacheEntityResponse {

    public static final Success INSTANCE = new Success();

    private Success() {
      //singleton
    }

    @Override
    public Type getType() {
      return Type.SUCCESS;
    }

  }

  public static class Failure extends EhcacheEntityResponse {

    private final ClusterException cause;

    Failure(ClusterException cause) {
      this.cause = cause;
    }

    @Override
    public Type getType() {
      return Type.FAILURE;
    }

    public ClusterException getCause() {
      return cause;
    }

  }

  public static class GetResponse extends EhcacheEntityResponse {

    private final Chain chain;

    GetResponse(Chain chain) {
      this.chain = chain;
    }

    @Override
    public Type getType() {
      return Type.GET_RESPONSE;
    }

    public Chain getChain() {
      return chain;
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
    public Type getType() {
      return Type.HASH_INVALIDATION_DONE;
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
    public Type getType() {
      return Type.ALL_INVALIDATION_DONE;
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
    public Type getType() {
      return Type.SERVER_INVALIDATE_HASH;
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
    public Type getType() {
      return Type.CLIENT_INVALIDATE_HASH;
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
    public Type getType() {
      return Type.CLIENT_INVALIDATE_ALL;
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

    @Override
    public Type getType() {
      return Type.MAP_VALUE;
    }

    public Object getValue() {
      return this.value;
    }
  }

}
