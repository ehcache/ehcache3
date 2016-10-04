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
public class RetirementMessage extends EhcacheEntityMessage {

  public enum RetirementOp {
    SERVERSTORE_RETIRE((byte) 31),
    RETIRE((byte) 32)
    ;

    private final byte retireOpCode;

    RetirementOp(byte retireOpCode) {
      this.retireOpCode = retireOpCode;
    }

    public byte getRetireOpCode() {
      return retireOpCode;
    }


    public static RetirementOp getRetirementOp(byte retireOpCode) {
      switch (retireOpCode) {
        case 31:
          return SERVERSTORE_RETIRE;
        case 32:
          return RETIRE;
        default:
          throw new IllegalArgumentException("Retirement operation not defined for : " + retireOpCode);
      }
    }
  }

  private final UUID clientId;
  private final long msgId;

  public RetirementMessage(long msgId, UUID clientId) {
    this.msgId = msgId;
    this.clientId = clientId;
  }

  @Override
  public Type getType() {
    return Type.RETIREMENT_OP;
  }

  @Override
  public byte getOpCode() {
    return operation().getRetireOpCode();
  }

  @Override
  public void setId(long id) {
    throw new UnsupportedOperationException("This method is not supported on retirement message");
  }

  public RetirementOp operation() {
    return RetirementOp.RETIRE;
  }

  public long getId() {
    return msgId;
  }

  public UUID getClientId() {
    return clientId;
  }

  public static class ServerStoreRetirementMessage extends RetirementMessage implements ConcurrentEntityMessage {

    private final String cacheId;
    private final long key;
    private final Chain chain;

    public ServerStoreRetirementMessage(String cacheId, long key, Chain chain, long msgId, UUID clientId) {
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
    public RetirementOp operation() {
      return RetirementOp.SERVERSTORE_RETIRE;
    }

    @Override
    public int concurrencyKey() {
      return (int) (this.cacheId.hashCode() + key);
    }
  }
}
