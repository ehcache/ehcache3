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

    GET((byte) 0),
    GETANDAPPEND((byte) 1),
    APPEND((byte) 2),
    REPLACE((byte) 3);

    private final byte storeOpCode;

    ServerStoreOp(byte storeOpCode) {
      this.storeOpCode = storeOpCode;
    }

    public byte getStoreOpCode() {
      return this.storeOpCode;
    }
  }

  private final String cacheId;
  private final long key;

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
      return ServerStoreOp.GETANDAPPEND;
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

}

