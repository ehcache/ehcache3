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

public abstract class ServerStoreOpMessage extends EhcacheOperationMessage {

  private ServerStoreOpMessage() {
  }

  public static abstract class KeyBasedServerStoreOpMessage extends ServerStoreOpMessage  implements ConcurrentEntityMessage {

    private final long key;

    KeyBasedServerStoreOpMessage(final long key) {
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

    public GetMessage(long key) {
      super(key);
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.GET_STORE;
    }
  }

  public static class GetAndAppendMessage extends KeyBasedServerStoreOpMessage {

    private final ByteBuffer payload;

    public GetAndAppendMessage(long key, ByteBuffer payload) {
      super(key);
      this.payload = payload;
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

    public AppendMessage(long key, ByteBuffer payload) {
      super(key);
      this.payload = payload;
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

    public ReplaceAtHeadMessage(long key, Chain expect, Chain update) {
      super(key);
      this.expect = expect;
      this.update = update;
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

    public ClientInvalidationAck(long key, int invalidationId) {
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

    public ClientInvalidationAllAck(int invalidationId) {
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

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.CLEAR;
    }
  }

}

