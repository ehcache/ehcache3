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

package org.ehcache.clustered.server.internal.messages;

import org.ehcache.clustered.common.internal.messages.ConcurrentEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.store.Chain;

import java.util.UUID;

/**
 * This message is sent by the Active Entity to Passive Entity.
 */
public abstract class PassiveReplicationMessage extends EhcacheOperationMessage {

  @Override
  public void setId(long id) {
    throw new UnsupportedOperationException("This method is not supported on replication message");
  }

  public static class ChainReplicationMessage extends PassiveReplicationMessage implements ConcurrentEntityMessage {

    private final UUID clientId;
    private final long key;
    private final Chain chain;
    private final long currentTransactionId;
    private final long oldestTransactionId;

    public ChainReplicationMessage(long key, Chain chain, long currentTransactionId, long oldestTransactionId, UUID clientId) {
      this.clientId = clientId;
      this.currentTransactionId = currentTransactionId;
      this.oldestTransactionId = oldestTransactionId;
      this.key = key;
      this.chain = chain;
    }

    public UUID getClientId() {
      return clientId;
    }

    public long getKey() {
      return key;
    }

    public Chain getChain() {
      return chain;
    }

    public long getId() {
      return currentTransactionId;
    }

    public long getOldestTransactionId() {
      return oldestTransactionId;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.CHAIN_REPLICATION_OP;
    }

    @Override
    public long concurrencyKey() {
      return key;
    }
  }

  public static class ClearInvalidationCompleteMessage extends PassiveReplicationMessage {

    public ClearInvalidationCompleteMessage() {
    }

    @Override
    public long getId() {
      throw new UnsupportedOperationException("Not supported for ClearInvalidationCompleteMessage");
    }

    @Override
    public UUID getClientId() {
      throw new UnsupportedOperationException("Not supported for ClearInvalidationCompleteMessage");
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.CLEAR_INVALIDATION_COMPLETE;
    }
  }

  public static class InvalidationCompleteMessage extends PassiveReplicationMessage implements ConcurrentEntityMessage {

    private final long key;

    public InvalidationCompleteMessage(long key) {
      this.key = key;
    }

    @Override
    public long concurrencyKey() {
      return key;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.INVALIDATION_COMPLETE;
    }

    public long getKey() {
      return key;
    }

    @Override
    public long getId() {
      throw new UnsupportedOperationException("Not supported for InvalidationCompleteMessage");
    }

    @Override
    public UUID getClientId() {
      throw new UnsupportedOperationException("Not supported for InvalidationCompleteMessage");
    }
  }
}
