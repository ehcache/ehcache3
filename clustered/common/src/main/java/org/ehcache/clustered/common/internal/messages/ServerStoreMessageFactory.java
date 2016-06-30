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

import org.ehcache.clustered.common.internal.store.Chain;

import java.nio.ByteBuffer;

public class ServerStoreMessageFactory {

  private final String cacheId;

  public ServerStoreMessageFactory(String cacheId) {
    this.cacheId = cacheId;
  }

  public EhcacheEntityMessage getOperation(long key) {
    return new ServerStoreOpMessage.GetMessage(this.cacheId, key);
  }

  public EhcacheEntityMessage getAndAppendOperation(long key, ByteBuffer payload) {
    return new ServerStoreOpMessage.GetAndAppendMessage(this.cacheId, key, payload);
  }

  public EhcacheEntityMessage appendOperation(long key, ByteBuffer payload) {
    return new ServerStoreOpMessage.AppendMessage(this.cacheId, key, payload);
  }

  public EhcacheEntityMessage replaceAtHeadOperation(long key, Chain expect, Chain update) {
    return new ServerStoreOpMessage.ReplaceAtHeadMessage(this.cacheId, key, expect, update);
  }

  public EhcacheEntityMessage clientInvalidationAck(int invalidationId) {
    return new ServerStoreOpMessage.ClientInvalidationAck(this.cacheId, invalidationId);
  }

  public EhcacheEntityMessage clearOperation() {
    return new ServerStoreOpMessage.ClearMessage(this.cacheId);
  }

  public String getCacheId() {
    return cacheId;
  }

}

