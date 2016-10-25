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

public abstract class ServerStoreOpMessage extends EhcacheEntityMessage {
  public enum ServerStoreOp {

    GET_AND_APPEND((byte) 11),
    APPEND((byte) 12),
    REPLACE((byte) 13),
    CLIENT_INVALIDATION_ACK((byte) 14),
    CLEAR((byte) 15),
    GET((byte) 16),
    ;

    private final byte storeOpCode;

    ServerStoreOp(byte storeOpCode) {
      this.storeOpCode = storeOpCode;
    }

    public byte getStoreOpCode() {
      return this.storeOpCode;
    }

    public static ServerStoreOp getServerStoreOp(byte storeOpCode) {
      switch (storeOpCode) {
        case 11:
          return GET_AND_APPEND;
        case 12:
          return APPEND;
        case 13:
          return REPLACE;
        case 14:
          return CLIENT_INVALIDATION_ACK;
        case 15:
          return CLEAR;
        case 16:
          return GET;
        default:
          throw new IllegalArgumentException("Store operation not defined for : " + storeOpCode);
      }
    }

  }

  protected UUID clientId;
  protected long id = NOT_REPLICATED;

  @Override
  public UUID getClientId() {
    if (clientId == null) {
      throw new AssertionError("Client Id is not supported for message type " + this.operation() );
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

  private final String cacheId;

  private ServerStoreOpMessage(String cacheId) {
    this.cacheId = cacheId;
  }

  public String getCacheId() {
    return cacheId;
  }

  @Override
  public Type getType() {
    return Type.SERVER_STORE_OP;
  }

  public abstract ServerStoreOp operation();

  @Override
  public byte getOpCode() {
    return operation().getStoreOpCode();
  }

  @Override
  public String toString() {
    return getType() + "#" + operation();
  }

  public static abstract class KeyBasedServerStoreOpMessage extends ServerStoreOpMessage  implements ConcurrentEntityMessage {

    private final long key;

    KeyBasedServerStoreOpMessage(final String cacheId, final long key) {
      super(cacheId);
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

    GetMessage(String cacheId, long key) {
      super(cacheId, key);
    }

    @Override
    public ServerStoreOp operation() {
      return ServerStoreOp.GET;
    }
  }

  public static class GetAndAppendMessage extends KeyBasedServerStoreOpMessage {

    private final ByteBuffer payload;

    GetAndAppendMessage(String cacheId, long key, ByteBuffer payload, UUID clientId) {
      super(cacheId, key);
      this.payload = payload;
      this.clientId = clientId;
    }

    @Override
    public ServerStoreOp operation() {
      return ServerStoreOp.GET_AND_APPEND;
    }

    public ByteBuffer getPayload() {
      return payload;
    }

  }

  public static class AppendMessage extends KeyBasedServerStoreOpMessage {

    private final ByteBuffer payload;

    AppendMessage(String cacheId, long key, ByteBuffer payload, UUID clientId) {
      super(cacheId, key);
      this.payload = payload;
      this.clientId = clientId;
    }

    @Override
    public ServerStoreOp operation() {
      return ServerStoreOp.APPEND;
    }

    public ByteBuffer getPayload() {
      return payload;
    }

  }

  public static class ReplaceAtHeadMessage extends KeyBasedServerStoreOpMessage {

    private final Chain expect;
    private final Chain update;

    ReplaceAtHeadMessage(String cacheId, long key, Chain expect, Chain update, UUID clientId) {
      super(cacheId, key);
      this.expect = expect;
      this.update = update;
      this.clientId = clientId;
    }

    @Override
    public ServerStoreOp operation() {
      return ServerStoreOp.REPLACE;
    }

    public Chain getExpect() {
      return expect;
    }

    public Chain getUpdate() {
      return update;
    }
  }

  public static class ClientInvalidationAck extends ServerStoreOpMessage {

    private final int invalidationId;

    ClientInvalidationAck(String cacheId, int invalidationId) {
      super(cacheId);
      this.invalidationId = invalidationId;
    }

    public int getInvalidationId() {
      return invalidationId;
    }

    @Override
    public ServerStoreOp operation() {
      return ServerStoreOp.CLIENT_INVALIDATION_ACK;
    }
  }

  public static class ClearMessage extends ServerStoreOpMessage {

    ClearMessage(String cacheId, UUID clientId) {
      super(cacheId);
      this.clientId = clientId;
    }

    @Override
    public ServerStoreOp operation() {
      return ServerStoreOp.CLEAR;
    }
  }

}

