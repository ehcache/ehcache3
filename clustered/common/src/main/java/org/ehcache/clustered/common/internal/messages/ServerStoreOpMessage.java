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
import java.util.UUID;

public abstract class ServerStoreOpMessage extends EhcacheOperationMessage {

  protected UUID clientId;
  protected long id = NOT_REPLICATED;

  @Override
  public UUID getClientId() {
    if (clientId == null) {
      throw new AssertionError("Client Id is not supported for message type " + this.getMessageType() );
    }
    return this.clientId;
  }

  @Override
  public long getId() {
    return this.id;
  }

  @Override
  public void setId(long id) {
    this.id = id;
  }

  private ServerStoreOpMessage() {
  }

  public static abstract class KeyBasedServerStoreOpMessage extends ServerStoreOpMessage  implements ConcurrentEntityMessage {

    private final long key;

    KeyBasedServerStoreOpMessage(final long key) {
      super();
      this.key = key;
    }

    public long getKey() {
      return key;
    }

    @Override
    public long concurrencyKey() {
      return key;
    }
  }

  public static class GetMessage extends KeyBasedServerStoreOpMessage {

    GetMessage(long key) {
      super(key);
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.GET_STORE;
    }
  }

  public static class GetAndAppendMessage extends KeyBasedServerStoreOpMessage {

    private final ByteBuffer payload;

    GetAndAppendMessage(long key, ByteBuffer payload, UUID clientId) {
      super(key);
      this.payload = payload;
      this.clientId = clientId;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.GET_AND_APPEND;
    }

    public ByteBuffer getPayload() {
      return payload;
    }

  }

  public static class AppendMessage extends KeyBasedServerStoreOpMessage {

    private final ByteBuffer payload;

    AppendMessage(long key, ByteBuffer payload, UUID clientId) {
      super(key);
      this.payload = payload;
      this.clientId = clientId;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.APPEND;
    }

    public ByteBuffer getPayload() {
      return payload;
    }

  }

  public static class ReplaceAtHeadMessage extends KeyBasedServerStoreOpMessage {

    private final Chain expect;
    private final Chain update;

    ReplaceAtHeadMessage(long key, Chain expect, Chain update, UUID clientId) {
      super(key);
      this.expect = expect;
      this.update = update;
      this.clientId = clientId;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.REPLACE;
    }

    public Chain getExpect() {
      return expect;
    }

    public Chain getUpdate() {
      return update;
    }
  }

  public static class ClientInvalidationAck extends KeyBasedServerStoreOpMessage {

    private final int invalidationId;

    ClientInvalidationAck(long key, int invalidationId) {
      super(key);
      this.invalidationId = invalidationId;
    }

    public int getInvalidationId() {
      return invalidationId;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.CLIENT_INVALIDATION_ACK;
    }
  }

  public static class ClientInvalidationAllAck extends ServerStoreOpMessage {

    private final int invalidationId;

    ClientInvalidationAllAck(int invalidationId) {
      super();
      this.invalidationId = invalidationId;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.CLIENT_INVALIDATION_ALL_ACK;
    }

    public int getInvalidationId() {
      return invalidationId;
    }
  }

  public static class ClearMessage extends ServerStoreOpMessage {

    ClearMessage(UUID clientId) {
      super();
      this.clientId = clientId;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.CLEAR;
    }
  }

}

