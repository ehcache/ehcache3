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
import java.nio.charset.Charset;

/**
 */
public abstract class ServerStoreOpMessage extends EhcacheEntityMessage {
  public enum ServerStoreOp {

    GET((byte) 0),
    GET_AND_APPEND((byte) 1),
    APPEND((byte) 2),
    REPLACE((byte) 3);

    private final byte storeOpCode;

    ServerStoreOp(byte storeOpCode) {
      this.storeOpCode = storeOpCode;
    }

    public byte getStoreOpCode() {
      return this.storeOpCode;
    }

    public static ServerStoreOp getServerStoreOp(byte storeOpCode) {
      switch (storeOpCode) {
        case 0:
          return GET;
        case 1:
          return GET_AND_APPEND;
        case 2:
          return APPEND;
        case 3:
          return REPLACE;
        default:
          throw new IllegalArgumentException("Store operation not defined for : " + storeOpCode);
      }
    }

  }

  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private final String cacheId;
  private final long key;

  protected static final byte MSG_TYPE_OFFSET = 1;
  protected static final byte STORE_OP_CODE_OFFSET = 1;
  protected static final byte CACHE_ID_LEN_OFFSET = 4;
  protected static final byte KEY_OFFSET = 8;

  protected ServerStoreOpMessage(String cacheId, long key) {
    this.cacheId = cacheId;
    this.key = key;
  }

  @Override
  public Type getType() {
    return Type.SERVER_STORE_OP;
  }

  public String getCacheId() {
    return cacheId;
  }

  public long getKey() {
    return key;
  }

  public abstract ServerStoreOp operation();

  public byte[] encode() {
    ByteBuffer encodedMsg = ByteBuffer.allocate(MSG_TYPE_OFFSET + STORE_OP_CODE_OFFSET + CACHE_ID_LEN_OFFSET + KEY_OFFSET + getCacheId().length());
    encodedMsg.put(EhcacheEntityMessage.Type.SERVER_STORE_OP.getOpCode());
    encodedMsg.putInt(this.cacheId.length());
    encodedMsg.put(this.cacheId.getBytes(UTF_8));
    encodedMsg.putLong(this.key);
    encodedMsg.put(this.operation().getStoreOpCode());
    return encodedMsg.array();
  }

  public static class GetMessage extends ServerStoreOpMessage {

    GetMessage(String cacheId, long key) {
      super(cacheId, key);
    }

    @Override
    public ServerStoreOp operation() {
      return ServerStoreOp.GET;
    }

    public byte[] encode() {
      return super.encode();
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

    public byte[] encode() {
      byte[] encodedMsg = super.encode();
      byte[] msg = new byte[encodedMsg.length + this.payload.remaining()];
      ByteBuffer buffer = ByteBuffer.wrap(msg);
      buffer.put(encodedMsg);
      buffer.put(this.payload);
      return buffer.array();
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

    public byte[] encode() {
      byte[] encodedMsg = super.encode();
      byte[] msg = new byte[encodedMsg.length + this.payload.remaining()];
      ByteBuffer buffer = ByteBuffer.wrap(msg);
      buffer.put(encodedMsg);
      buffer.put(this.payload);
      return buffer.array();
    }
  }

  public static class ReplaceAtHeadMessage extends ServerStoreOpMessage {

    private static final byte CHAIN_LEN_OFFSET = 4;

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

    @Override
    public byte[] encode() {
      byte[] encodedExpectedChain = ChainCodec.encode(this.expect);
      byte[] encodedUpdatedChain = ChainCodec.encode(this.update);
      byte[] encodedMsg = super.encode();
      byte[] msg = new byte[encodedExpectedChain.length + encodedUpdatedChain.length + encodedMsg.length + 2 * CHAIN_LEN_OFFSET];
      ByteBuffer buffer = ByteBuffer.wrap(msg);
      buffer.put(encodedMsg);
      buffer.putInt(encodedExpectedChain.length);
      buffer.put(encodedExpectedChain);
      buffer.putInt(encodedUpdatedChain.length);
      buffer.put(encodedUpdatedChain);
      return buffer.array();
    }
  }
}

