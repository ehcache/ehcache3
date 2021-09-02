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

package org.ehcache.clustered.common.internal.store;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

/**
 * Defines Server Side data structure for storing
 * cache data.
 * This is conceptually a multi-map which has hash as the key
 * and a {@link Chain} as the mapping
 * Chain is list of {@link Element}s such that the same key has
 * multiple mappings attached.
 *
 * Physical representation looks something like this :
 *
 *  hash -> |payLoad1| - |payLoad2| - |payLoad3|
 *  hash -> |payLoad1| - |payLoad2|
 *
 *  This only allows append & replaceAtHead operations rather than
 *  normal map operations.
 *
 */
public interface ServerStore {

  /**
   * Returns the Chain associated with the provided hash.
   * An empty Chain if no mapping exists.
   *
   * @param key hashcode of the key
   * @return the {@link Chain} associated with the hash
   *
   * @throws TimeoutException if the get exceeds the timeout configured for read operations
   */
  Chain get(long key) throws TimeoutException;

  /**
   * Appends the provided binary to Chain associated with key atomically.
   * While appending, the payLoad is stored in {@link Element}.
   * Note that the {@code payLoad}'s position and limit are left untouched.
   *
   * @param key to which the payLoad has to be appended
   * @param payLoad to be appended
   *
   * @throws TimeoutException if the append exceeds the timeout configured for write operations
   */
  void append(long key, ByteBuffer payLoad) throws TimeoutException;

  /**
   * The Chain associated with key, previous to append is returned.
   * An empty Chain if no mapping existed previously.
   * The operation atomically appends payLoad and returns the previously associated Chain
   * with the key.
   *  Following block of instructions are atomically performed.
   *  {
   *    Chain prevChain = get(key);
   *    append(key, payLoad);
   *    return prevChain;
   *  }
   * Note that the {@code payLoad}'s position and limit are left untouched.
   *
   * @param key to which the payLoad has to be appended
   * @param payLoad to be appended
   * @return the Chain associated with the key before payLoad was appended
   *
   * @throws TimeoutException if the get exceeds the timeout configured for read operations
   */
  Chain getAndAppend(long key, ByteBuffer payLoad) throws TimeoutException;

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
   * @param key for which Chain has to be replaced
   * @param expect of the original Chain which this operation intends to replaceAtHead
   * @param update the new Chain to be replaced
   */
  void replaceAtHead(long key, Chain expect, Chain update);

  /**
   * Removes all the mappings from this store. But this operation is not atomic.
   * If appends are happening in parallel, this operation does not guarantee an
   * empty store on the completion of this operation.
   *
   * @throws TimeoutException if the get exceeds the timeout configured for write operations
   */
  void clear() throws TimeoutException;

  /**
   * Returns an iterator over the chains.
   *
   * @return an chain iterator.
   */
  Iterator<Chain> iterator() throws TimeoutException;
}
