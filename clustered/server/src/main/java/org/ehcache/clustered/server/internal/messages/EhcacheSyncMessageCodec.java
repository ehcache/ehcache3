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

import org.ehcache.clustered.common.internal.messages.ChainCodec;
import org.ehcache.clustered.common.internal.messages.CodecUtil;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Util;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.SyncMessageCodec;

import java.nio.ByteBuffer;

public class EhcacheSyncMessageCodec implements SyncMessageCodec<EhcacheEntityMessage> {

  private static final byte OPCODE_SIZE = 1;
  private static final byte KEY_SIZE = 8;
  private static final byte CACHE_ID_LEN_SIZE = 4;

  private final ChainCodec chainCodec = new ChainCodec();

  @Override
  public byte[] encode(final int concurrencyKey, final EhcacheEntityMessage message) throws MessageCodecException {
    if(message.getType() == EhcacheEntityMessage.Type.SYNC_OP) {
      EntitySyncMessage syncMessage = (EntitySyncMessage)message;
      switch (syncMessage.operation()) {
        case STATE: {
          byte[] encodedMsg = Util.marshall(syncMessage);
          ByteBuffer buffer = ByteBuffer.allocate(OPCODE_SIZE + encodedMsg.length);
          buffer.put(syncMessage.getOpCode());
          buffer.put(encodedMsg);
          return buffer.array();
        }
        case DATA: {
          EntityDataSyncMessage dataSyncMessage = (EntityDataSyncMessage)message;
          String cacheId = dataSyncMessage.getCacheId();
          byte[] encodedChain = chainCodec.encode(dataSyncMessage.getChain());
          ByteBuffer buffer = ByteBuffer.allocate(OPCODE_SIZE + KEY_SIZE + CACHE_ID_LEN_SIZE +
                                                  2 * cacheId.length() + encodedChain.length);
          buffer.put(dataSyncMessage.getOpCode());
          buffer.putLong(dataSyncMessage.getKey());
          buffer.putInt(cacheId.length());
          CodecUtil.putStringAsCharArray(buffer, cacheId);
          buffer.put(encodedChain);
          return buffer.array();
        }
        default:
          throw new IllegalArgumentException(this.getClass().getName() + " can not encode " + syncMessage.operation());
      }
    } else {
      throw new IllegalArgumentException(this.getClass().getName() + " can not encode " + message + " which is not a " + EntityStateSyncMessage.class);
    }
  }

  @Override
  public EntitySyncMessage decode(final int concurrencyKey, final byte[] payload) throws MessageCodecException {
    ByteBuffer message = ByteBuffer.wrap(payload);
    EntitySyncMessage.SyncOp syncOp = EntitySyncMessage.SyncOp.getSyncOp(message.get());
    switch (syncOp) {
      case STATE: {
        byte[] encodedMsg = new byte[message.capacity() - OPCODE_SIZE];
        message.get(encodedMsg, 0, encodedMsg.length);
        return  (EntityStateSyncMessage) Util.unmarshall(encodedMsg);
      }
      case DATA: {
        long key = message.getLong();
        int cacheIdLength = message.getInt();
        String cacheId = CodecUtil.getStringFromBuffer(message, cacheIdLength);
        int chainPayloadSize = message.remaining();
        byte[] chainPayload = new byte[chainPayloadSize];
        message.get(chainPayload);
        Chain chain = chainCodec.decode(chainPayload);
        return new EntityDataSyncMessage(cacheId, key, chain);
      }
      default:
        throw new IllegalArgumentException("EntityStateSyncMessage operation not defined for : " + syncOp);
    }
  }
}
