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

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.ServerStore;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;

/**
 * @author Ludovic Orban
 */
public interface ServerStoreProxy extends ServerStore {

  /**
   * {@inheritDoc}
   * <p>
   * {@code ServerStoreProxy} instances return {@link ChainEntry} instances that support mutation of the associated store.
   *
   * @return the associated chain entry
   */
  @Override
  ChainEntry get(long key) throws TimeoutException;

  /**
   * {@inheritDoc}
   * <p>
   * {@code ServerStoreProxy} instances return {@link ChainEntry} instances that support mutation of the associated store.
   *
   * @return the associated chain entry
   */
  @Override
  ChainEntry getAndAppend(long key, ByteBuffer payLoad) throws TimeoutException;

  /**
   * The invalidation listener
   */
  interface ServerCallback {
    /**
     * Callback for invalidation of hash requests
     *
     * @param hash the hash of the keys to invalidate
     */
    void onInvalidateHash(long hash);

    /**
     * Callback for invalidation of all requests
     */
    void onInvalidateAll();

    void compact(ChainEntry chain);

    default void compact(ChainEntry chain, long hash) {
      compact(chain);
    }
  }

  /**
   * Gets the identifier linking a client-side cache to a {@code ServerStore} instance.
   *
   * @return the cache identifier
   */
  String getCacheId();

  /**
   * Closes this proxy.
   */
  void close();

  interface ChainEntry extends Chain {

    /**
     * Appends the provided binary to this Chain
     * While appending, the payLoad is stored in {@link Element}.
     * Note that the {@code payLoad}'s position and limit are left untouched.
     *
     * @param payLoad to be appended
     *
     * @throws TimeoutException if the append exceeds the timeout configured for write operations
     */
    void append(ByteBuffer payLoad) throws TimeoutException;


    /**
     * Replaces the provided Chain with the equivalent Chain present at the head.
     * This operation is not guaranteed to succeed.
     * The replaceAtHead is successful iff the Chain associated with the key has
     * a sub-sequence of elements present as expected..
     *
     * If below mapping is present:
     *
     *    hash -> |payLoadA| - |payLoadB| - |payLoadC|
     *
     * And replaceAtHead(hash, |payLoadA| - |payLoadB| - |payLoadC|, |payLoadC'|) is invoked
     * then this operation will succeed & the final mapping would be:
     *
     *    hash -> |payLoadC'|
     *
     * The same operation will also succeed if the mapping was modified by the time replace was invoked to
     *
     *    hash -> |payLoadA| - |payLoadB| - |payLoadC| - |payLoadD|
     *
     * Though the final mapping would be:
     *
     *    hash -> |payLoadC'| - |payLoadD|
     *
     * Failure case:
     *
     * But before replaceAtHead if it was modified to :
     *
     *    hash -> |payLoadC"| - |payLoadD|
     *
     * then replaceAtHead(hash, |payLoadA| - |payLoadB| - |payLoadC|, |payLoadC'|) will be ignored.
     * Note that the payload's position and limit of all elements of both chains are left untouched.
     *
     * @param equivalent the new Chain to be replaced
     */
    void replaceAtHead(Chain equivalent);
  }
}
