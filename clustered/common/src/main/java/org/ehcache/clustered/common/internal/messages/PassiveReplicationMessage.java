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

import java.util.UUID;

/**
 * This message is sent by the Active Entity to Passive Entity.
 */
public abstract class PassiveReplicationMessage extends EhcacheEntityMessage {

  public enum ReplicationOp {
    CHAIN_REPLICATION_OP((byte) 41),
    CLIENTID_TRACK_OP((byte) 42),
    CLEAR_INVALIDATION_COMPLETE((byte) 43),
    INVALIDATION_COMPLETE((byte) 44),
    SERVER_STORE_LIFECYCLE_REPLICATION_OP((byte) 45)
    ;

    private final byte replicationOpCode;

    ReplicationOp(byte replicationOpCode) {
      this.replicationOpCode = replicationOpCode;
    }

    public byte getReplicationOpCode() {
      return replicationOpCode;
    }


    public static ReplicationOp getReplicationOp(byte replicationOpCode) {
      switch (replicationOpCode) {
        case 41:
          return CHAIN_REPLICATION_OP;
        case 42:
          return CLIENTID_TRACK_OP;
        case 43:
          return CLEAR_INVALIDATION_COMPLETE;
        case 44:
          return INVALIDATION_COMPLETE;
        case 45:
          return SERVER_STORE_LIFECYCLE_REPLICATION_OP;
        default:
          throw new IllegalArgumentException("Replication operation not defined for : " + replicationOpCode);
      }
    }
  }

  @Override
  public Type getType() {
    return Type.REPLICATION_OP;
  }

  @Override
  public byte getOpCode() {
    return operation().getReplicationOpCode();
  }

  @Override
  public void setId(long id) {
    throw new UnsupportedOperationException("This method is not supported on replication message");
  }

  public abstract ReplicationOp operation();

  public static class ClientIDTrackerMessage extends PassiveReplicationMessage {
    private final UUID clientId;
    private final long msgId;

    public ClientIDTrackerMessage(long msgId, UUID clientId) {
      this.msgId = msgId;
      this.clientId = clientId;
    }

    public ReplicationOp operation() {
      return ReplicationOp.CLIENTID_TRACK_OP;
    }
    public long getId() {
      return msgId;
    }

    public UUID getClientId() {
      return clientId;
    }
  }

  public static class ChainReplicationMessage extends ClientIDTrackerMessage implements ConcurrentEntityMessage {

    private final String cacheId;
    private final long key;
    private final Chain chain;

    public ChainReplicationMessage(String cacheId, long key, Chain chain, long msgId, UUID clientId) {
      super(msgId, clientId);
      this.cacheId = cacheId;
      this.key = key;
      this.chain = chain;
    }

    public String getCacheId() {
      return this.cacheId;
    }

    public long getKey() {
      return key;
    }

    public Chain getChain() {
      return chain;
    }

    @Override
    public ReplicationOp operation() {
      return ReplicationOp.CHAIN_REPLICATION_OP;
    }

    @Override
    public long concurrencyKey() {
      return key;
    }
  }

  public static class ClearInvalidationCompleteMessage extends PassiveReplicationMessage implements ConcurrentEntityMessage {
    private final String cacheId;

    public ClearInvalidationCompleteMessage(String cacheId) {
      this.cacheId = cacheId;
    }

    @Override
    public long concurrencyKey() {
      return this.cacheId.hashCode();
    }

    @Override
    public long getId() {
      throw new UnsupportedOperationException("Not supported for ClearInvalidationCompleteMessage");
    }

    @Override
    public UUID getClientId() {
      throw new UnsupportedOperationException("Not supported for ClearInvalidationCompleteMessage");
    }

    public ReplicationOp operation() {
      return ReplicationOp.CLEAR_INVALIDATION_COMPLETE;
    }

    public String getCacheId() {
      return cacheId;
    }
  }

  public static class InvalidationCompleteMessage extends ClearInvalidationCompleteMessage {

    private final long key;

    public InvalidationCompleteMessage(String cacheId, long key) {
      super(cacheId);
      this.key = key;
    }

    @Override
    public long concurrencyKey() {
      return (getCacheId().hashCode() + key);
    }

    public ReplicationOp operation() {
      return ReplicationOp.INVALIDATION_COMPLETE;
    }

    public long getKey() {
      return key;
    }
  }

  public static class ServerStoreLifeCycleReplicationMessage extends ClientIDTrackerMessage {

    private final LifecycleMessage message;

    public ServerStoreLifeCycleReplicationMessage(LifecycleMessage message) {
      super(message.getId(), message.getClientId());
      this.message = message;
    }

    public LifecycleMessage getMessage() {
      return message;
    }

    @Override
    public ReplicationOp operation() {
      return ReplicationOp.SERVER_STORE_LIFECYCLE_REPLICATION_OP;
    }
  }
}
