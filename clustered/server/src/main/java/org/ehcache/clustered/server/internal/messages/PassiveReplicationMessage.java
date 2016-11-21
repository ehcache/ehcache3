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

import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.ConcurrentEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
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

  public static class ClientIDTrackerMessage extends PassiveReplicationMessage {
    private final UUID clientId;

    public ClientIDTrackerMessage(UUID clientId) {
      this.clientId = clientId;
    }

    public UUID getClientId() {
      return clientId;
    }

    @Override
    public long getId() {
      throw new UnsupportedOperationException("Not supported for ClientIDTrackerMessage");
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.CLIENT_ID_TRACK_OP;
    }
  }

  public static class ChainReplicationMessage extends ClientIDTrackerMessage implements ConcurrentEntityMessage {

    private final String cacheId;
    private final long key;
    private final Chain chain;
    private final long msgId;

    public ChainReplicationMessage(String cacheId, long key, Chain chain, long msgId, UUID clientId) {
      super(clientId);
      this.msgId = msgId;
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

    public long getId() {
      return msgId;
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

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.CLEAR_INVALIDATION_COMPLETE;
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

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.INVALIDATION_COMPLETE;
    }

    public long getKey() {
      return key;
    }
  }

  public static class CreateServerStoreReplicationMessage extends ClientIDTrackerMessage {

    private final String storeName;
    private final ServerStoreConfiguration storeConfiguration;
    private final long msgId;

    public CreateServerStoreReplicationMessage(LifecycleMessage.CreateServerStore createMessage) {
      this(createMessage.getId(), createMessage.getClientId(), createMessage.getName(), createMessage.getStoreConfiguration());
    }

    public CreateServerStoreReplicationMessage(long msgId, UUID clientId, String storeName, ServerStoreConfiguration configuration) {
      super(clientId);
      this.msgId = msgId;
      this.storeName = storeName;
      this.storeConfiguration = configuration;
    }

    public String getStoreName() {
      return storeName;
    }

    public ServerStoreConfiguration getStoreConfiguration() {
      return storeConfiguration;
    }

    public long getId() {
      return msgId;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.CREATE_SERVER_STORE_REPLICATION;
    }
  }

  public static class DestroyServerStoreReplicationMessage extends ClientIDTrackerMessage {

    private final String storeName;
    private final long msgId;

    public DestroyServerStoreReplicationMessage(LifecycleMessage.DestroyServerStore destroyMessage) {
      this(destroyMessage.getId(), destroyMessage.getClientId(), destroyMessage.getName());
    }

    public DestroyServerStoreReplicationMessage(long msgId, UUID clientId, String storeName) {
      super(clientId);
      this.storeName = storeName;
      this.msgId = msgId;
    }

    public String getStoreName() {
      return storeName;
    }

    public long getId() {
      return msgId;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.DESTROY_SERVER_STORE_REPLICATION;
    }
  }
}
