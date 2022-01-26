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
import org.ehcache.clustered.common.internal.store.Element;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.ehcache.clustered.common.internal.util.ChainBuilder.chainFromList;

/**
 * This message is sent by the Active Entity to Passive Entity.
 */
public abstract class PassiveReplicationMessage extends EhcacheOperationMessage {

  public static class ChainReplicationMessage extends PassiveReplicationMessage implements ConcurrentEntityMessage {

    private final long clientId;
    private final long key;
    private final Chain chain;
    private final long transactionId;
    private final long oldestTransactionId;

    public ChainReplicationMessage(long key, Chain chain, long transactionId, long oldestTransactionId, long clientId) {
      this.clientId = clientId;
      this.transactionId = transactionId;
      this.oldestTransactionId = oldestTransactionId;
      this.key = key;
      this.chain = chain;
    }

    private Chain dropLastElement(Chain chain) {
      if (chain.isEmpty()) {
        return chain;
      } else {
        List<Element> elements = StreamSupport.stream(chain.spliterator(), false)
          .collect(Collectors.toList());
        elements.remove(elements.size() - 1); // remove last
        return chainFromList(elements);
      }
    }

    public long getClientId() {
      return clientId;
    }

    public long getTransactionId() {
      return transactionId;
    }

    public long getKey() {
      return key;
    }

    /**
     * @return chain that needs to be save in the store
     */
    public Chain getChain() {
      return chain;
    }

    /**
     * @return result that should be returned is the original message is sent again to this server after a failover
     */
    public Chain getResult() {
      return dropLastElement(chain);
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
  }
}
