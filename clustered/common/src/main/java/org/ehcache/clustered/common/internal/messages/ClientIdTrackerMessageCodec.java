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

import org.ehcache.clustered.common.internal.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.internal.messages.ClientIDTrackerMessage.ReplicationOp;
import org.ehcache.clustered.common.internal.messages.ClientIDTrackerMessage.ChainReplicationMessage;
import org.ehcache.clustered.common.internal.store.Chain;

import java.nio.ByteBuffer;
import java.util.UUID;

class ClientIDTrackerMessageCodec {

  private static final byte OP_CODE_SIZE = 1;
  private static final byte CACHE_ID_LEN_SIZE = 4;
  private static final byte KEY_SIZE = 8;
  private static final byte MESSAGE_ID_SIZE = 24;

  private ChainCodec chainCodec = new ChainCodec();

  public byte[] encode(ClientIDTrackerMessage message) {

    ByteBuffer encodedMsg;
    switch (message.operation()) {
      case CLIENTID_TRACK_OP:
        encodedMsg = ByteBuffer.allocate(OP_CODE_SIZE + MESSAGE_ID_SIZE);
        encodedMsg.put(message.getOpCode());
        encodedMsg.put(ClusteredEhcacheIdentity.serialize(message.getClientId()));
        encodedMsg.putLong(message.getId());
        return encodedMsg.array();
      case CHAIN_REPLICATION_OP:
        ChainReplicationMessage chainReplicationMessage = (ChainReplicationMessage)message;
        byte[] encodedChain = chainCodec.encode(chainReplicationMessage.getChain());
        int cacheIdLen = chainReplicationMessage.getCacheId().length();
        encodedMsg = ByteBuffer.allocate(OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + MESSAGE_ID_SIZE + encodedChain.length + 2 * cacheIdLen);
        encodedMsg.put(chainReplicationMessage.getOpCode());
        encodedMsg.put(ClusteredEhcacheIdentity.serialize(chainReplicationMessage.getClientId()));
        encodedMsg.putLong(chainReplicationMessage.getId());
        encodedMsg.putInt(cacheIdLen);
        CodecUtil.putStringAsCharArray(encodedMsg, chainReplicationMessage.getCacheId());
        encodedMsg.putLong(chainReplicationMessage.getKey());
        encodedMsg.put(encodedChain);
        return encodedMsg.array();
      default:
        throw new UnsupportedOperationException("This operation is not supported : " + message.operation());
    }

  }

  public EhcacheEntityMessage decode(byte[] payload) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
    ReplicationOp replicationOp = ReplicationOp.getReplicationOp(byteBuffer.get());
    UUID clientId = getClientId(byteBuffer);
    long msgId = byteBuffer.getLong();
    switch (replicationOp) {
      case CHAIN_REPLICATION_OP:
        int length = byteBuffer.getInt();
        String cacheId = CodecUtil.getStringFromBuffer(byteBuffer, length);
        long key = byteBuffer.getLong();
        byte[] encodedChain = new byte[byteBuffer.remaining()];
        byteBuffer.get(encodedChain);
        Chain chain = chainCodec.decode(encodedChain);
        return new ChainReplicationMessage(cacheId, key, chain, msgId, clientId);
      case CLIENTID_TRACK_OP:
        return new ClientIDTrackerMessage(msgId, clientId);
      default:
        throw new UnsupportedOperationException("This operation code is not supported : " + replicationOp);
    }
  }

  private static UUID getClientId(ByteBuffer payload) {
    long msb = payload.getLong();
    long lsb = payload.getLong();
    return new UUID(msb, lsb);
  }
}
