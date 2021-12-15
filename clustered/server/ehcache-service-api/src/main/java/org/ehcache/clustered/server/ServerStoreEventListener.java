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
package org.ehcache.clustered.server;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.server.offheap.InternalChain;

import java.nio.ByteBuffer;

/**
 * ServerStore event listener interface
 */
public interface ServerStoreEventListener {

  /**
   * Called when the ServerStore evicts a mapping.
   * <p/>
   * <b>Always fired</b>, even when events are not enabled, see: {@link ServerSideServerStore#enableEvents(boolean)}.
   * @param key the key of the evicted mapping
   * @param evictedChain the evicted chain.
   */
  void onEviction(long key, InternalChain evictedChain);

  /**
   * Called when the ServerStore appends to a mapping
   * <p/>
   * <b>Not always fired</b>, only when events are enabled, see: {@link ServerSideServerStore#enableEvents(boolean)}.
   * @param beforeAppend the chain as it was before the append
   * @param appended the appended operation
   */
  void onAppend(Chain beforeAppend, ByteBuffer appended);

}
