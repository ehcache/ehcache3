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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public abstract class EhcacheEntityResponse implements EntityResponse {

  public abstract EhcacheResponseType getResponseType();

  public static Success success() {
    return Success.INSTANCE;
  }

  public static class Success extends EhcacheEntityResponse {

    private static final Success INSTANCE = new Success();

    private Success() {
      //singleton
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.SUCCESS;
    }
  }

  public static Failure failure(ClusterException cause) {
    return new Failure(cause);
  }

  public static class Failure extends EhcacheEntityResponse {

    private final ClusterException cause;

    private Failure(ClusterException cause) {
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

  public static GetResponse getResponse(Chain chain) {
    return new GetResponse(chain);
  }

  public static class GetResponse extends EhcacheEntityResponse {

    private final Chain chain;

    private GetResponse(Chain chain) {
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

    private HashInvalidationDone(long key) {
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

    private AllInvalidationDone() {
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.ALL_INVALIDATION_DONE;
    }
  }

  public static ServerAppend serverAppend(ByteBuffer appended, Chain beforeAppend) {
    return new ServerAppend(appended, beforeAppend);
  }

  public static class ServerAppend extends EhcacheEntityResponse {
    private final ByteBuffer appended;
    private final Chain beforeAppend;

    ServerAppend(ByteBuffer appended, Chain beforeAppend) {
      this.appended = appended;
      this.beforeAppend = beforeAppend;
    }

    public ByteBuffer getAppended() {
      return appended;
    }

    public Chain getBeforeAppend() {
      return beforeAppend;
    }

    @Override
    public EhcacheResponseType getResponseType() {
      return EhcacheResponseType.SERVER_APPEND;
    }
  }

  public static ServerInvalidateHash serverInvalidateHash(long key, Chain evictedChain) {
    return new ServerInvalidateHash(key, evictedChain);
  }

  // this is fired when the server evicts a chain
  public static class ServerInvalidateHash extends EhcacheEntityResponse {
    private final long key;
    private final Chain evictedChain;

    private ServerInvalidateHash(long key, Chain evictedChain) {
      this.key = key;
      this.evictedChain = evictedChain;
    }

    public long getKey() {
      return key;
    }

    public Chain getEvictedChain() {
      return evictedChain;
    }

    @Override
    public final EhcacheResponseType getResponseType() {
      return EhcacheResponseType.SERVER_INVALIDATE_HASH;
    }
  }

  public static ClientInvalidateHash clientInvalidateHash(long key, int invalidationId) {
    return new ClientInvalidateHash(key, invalidationId);
  }

  // this is fired when a client modifies a chain (i.e.: on append)
  public static class ClientInvalidateHash extends EhcacheEntityResponse {
    private final long key;
    private final int invalidationId;

    private ClientInvalidateHash(long key, int invalidationId) {
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

    private ClientInvalidateAll(int invalidationId) {
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

    private MapValue(Object value) {
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

  public static PrepareForDestroy prepareForDestroy(Set<String> stores) {
    return new PrepareForDestroy(stores);
  }

  public static class PrepareForDestroy extends EhcacheEntityResponse {

    private final Set<String> stores;

    private PrepareForDestroy(Set<String> stores) {
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

  public static ResolveRequest resolveRequest(long key, Chain chain) {
    return new ResolveRequest(key, chain);
  }

  public static class ResolveRequest extends EhcacheEntityResponse {

    private final long key;
    private final Chain chain;

    ResolveRequest(long key, Chain chain) {
      this.key = key;
      this.chain = chain;
    }

    @Override
    public EhcacheResponseType getResponseType() {
      return EhcacheResponseType.RESOLVE_REQUEST;
    }

    public long getKey() {
      return key;
    }

    public Chain getChain() {
      return chain;
    }
  }

  public static LockSuccess lockSuccess(Chain chain) {
    return new LockSuccess(chain);
  }

  public static class LockSuccess extends EhcacheEntityResponse {

    private final Chain chain;

    LockSuccess(Chain chain) {
      this.chain = chain;
    }

    public Chain getChain() {
      return chain;
    }

    @Override
    public EhcacheResponseType getResponseType() {
      return EhcacheResponseType.LOCK_SUCCESS;
    }
  }

  public static LockFailure lockFailure() {
    return new LockFailure();
  }

  public static class LockFailure extends EhcacheEntityResponse {

    private LockFailure() {

    }

    @Override
    public EhcacheResponseType getResponseType() {
      return EhcacheResponseType.LOCK_FAILURE;
    }
  }

  public static IteratorBatch iteratorBatchResponse(UUID id, List<Chain> chains, boolean last) {
    return new IteratorBatch(id, chains, last);
  }

  public static class IteratorBatch extends EhcacheEntityResponse {

    private final UUID id;
    private final List<Chain> chains;
    private final boolean last;

    public IteratorBatch(UUID id, List<Chain> chains, boolean last) {
      this.id = id;
      this.chains = chains;
      this.last = last;
    }

    @Override
    public EhcacheResponseType getResponseType() {
      return EhcacheResponseType.ITERATOR_BATCH;
    }

    public boolean isLast() {
      return last;
    }

    public List<Chain> getChains() {
      return chains;
    }

    public UUID getIdentity() {
      return id;
    }
  }
}
