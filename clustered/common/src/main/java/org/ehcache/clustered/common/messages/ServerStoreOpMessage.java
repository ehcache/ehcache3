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
package org.ehcache.clustered.common.messages;

import org.ehcache.clustered.common.store.Chain;

import java.nio.ByteBuffer;

/**
 */
public abstract class ServerStoreOpMessage extends EhcacheEntityMessage {
  public enum ServerStoreOp {

    GET((byte) 10),
    GET_AND_APPEND((byte) 11),
    APPEND((byte) 12),
    REPLACE((byte) 13),
    CLIENT_INVALIDATE_HASH_ACK((byte) 14),
    CLEAR((byte) 15),
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
        case 10:
          return GET;
        case 11:
          return GET_AND_APPEND;
        case 12:
          return APPEND;
        case 13:
          return REPLACE;
        case 14:
          return CLIENT_INVALIDATE_HASH_ACK;
        case 15:
          return CLEAR;
        default:
          throw new IllegalArgumentException("Store operation not defined for : " + storeOpCode);
      }
    }

  }

  private final String cacheId;
  private final long key;

  private ServerStoreOpMessage(String cacheId, long key) {
    this.cacheId = cacheId;
    this.key = key;
  }

  public String getCacheId() {
    return cacheId;
  }

  public long getKey() {
    return key;
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

  public static class GetMessage extends ServerStoreOpMessage {

    GetMessage(String cacheId, long key) {
      super(cacheId, key);
    }

    @Override
    public ServerStoreOp operation() {
      return ServerStoreOp.GET;
    }
  }

  public static class GetAndAppendMessage extends ServerStoreOpMessage {

    private final ByteBuffer payload;

    GetAndAppendMessage(String cacheId, long key, ByteBuffer payload) {
      super(cacheId, key);
      this.payload = payload;
    }

    @Override
    public ServerStoreOp operation() {
      return ServerStoreOp.GET_AND_APPEND;
    }

    public ByteBuffer getPayload() {
      return payload;
    }

  }

  public static class AppendMessage extends ServerStoreOpMessage {

    private final ByteBuffer payload;

    AppendMessage(String cacheId, long key, ByteBuffer payload) {
      super(cacheId, key);
      this.payload = payload;
    }

    @Override
    public ServerStoreOp operation() {
      return ServerStoreOp.APPEND;
    }

    public ByteBuffer getPayload() {
      return payload;
    }

  }

  public static class ReplaceAtHeadMessage extends ServerStoreOpMessage {

    private final Chain expect;
    private final Chain update;

    ReplaceAtHeadMessage(String cacheId, long key, Chain expect, Chain update) {
      super(cacheId, key);
      this.expect = expect;
      this.update = update;
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


  public static class ClientInvalidateHashAck extends ServerStoreOpMessage {

    private final int invalidationId;

    public ClientInvalidateHashAck(String cacheId, long key, int invalidationId) {
      super(cacheId, key);
      this.invalidationId = invalidationId;
    }

    public int getInvalidationId() {
      return invalidationId;
    }

    @Override
    public ServerStoreOp operation() {
      return ServerStoreOp.CLIENT_INVALIDATE_HASH_ACK;
    }

    @Override
    public byte getOpCode() {
      return ServerStoreOp.CLIENT_INVALIDATE_HASH_ACK.getStoreOpCode();
    }
  }

  static class ClearMessage extends ServerStoreOpMessage {


    ClearMessage(final String cacheId) {
      super(cacheId, 0L);
    }

    @Override
    public ServerStoreOp operation() {
      return ServerStoreOp.CLEAR;
    }

    @Override
    public byte getOpCode() {
      return ServerStoreOp.CLEAR.getStoreOpCode();
    }

    @Override
    public long getKey() {
      throw new UnsupportedOperationException("Clear message does not have a key a parameter");
    }
  }

}

