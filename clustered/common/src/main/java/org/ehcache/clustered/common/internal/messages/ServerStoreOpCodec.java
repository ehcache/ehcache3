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
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.AppendMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ClearMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.GetAndAppendMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.GetMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ReplaceAtHeadMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ClientInvalidationAck;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ServerStoreOp;

import java.nio.ByteBuffer;
import java.util.UUID;

class ServerStoreOpCodec {

  private static final byte STORE_OP_CODE_SIZE = 1;
  private static final byte CACHE_ID_LEN_SIZE = 4;
  private static final byte KEY_SIZE = 8;
  private static final byte CHAIN_LEN_SIZE = 4;
  private static final byte INVALIDATION_ID_LEN_SIZE = 4;
  private static final byte MESSAGE_ID_SIZE = 24;

  private final ChainCodec chainCodec;

  ServerStoreOpCodec() {
    this.chainCodec = new ChainCodec();
  }

  public byte[] encode(ServerStoreOpMessage message) {
    // TODO: improve data send over n/w by optimizing cache Id
    ByteBuffer encodedMsg;
    int cacheIdLen = message.getCacheId().length();
    switch (message.operation()) {
      case GET:
        GetMessage getMessage = (GetMessage)message;
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + KEY_SIZE + 2 * cacheIdLen);
        encodedMsg.put(getMessage.getOpCode());
        encodedMsg.putLong(getMessage.getKey());
        CodecUtil.putStringAsCharArray(encodedMsg, getMessage.getCacheId());
        return encodedMsg.array();
      case APPEND:
        AppendMessage appendMessage = (AppendMessage)message;
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + MESSAGE_ID_SIZE + 2 * cacheIdLen + appendMessage
            .getPayload()
            .remaining());
        putCacheIdKeyAndOpCode(encodedMsg, appendMessage, appendMessage.getKey());
        encodedMsg.put(appendMessage.getPayload());
        return encodedMsg.array();
      case GET_AND_APPEND:
        GetAndAppendMessage getAndAppendMessage = (GetAndAppendMessage)message;
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + MESSAGE_ID_SIZE + 2 * cacheIdLen +
                                         getAndAppendMessage.getPayload().remaining());
        putCacheIdKeyAndOpCode(encodedMsg, getAndAppendMessage, getAndAppendMessage.getKey());
        encodedMsg.put(getAndAppendMessage.getPayload());
        return encodedMsg.array();
      case REPLACE:
        ReplaceAtHeadMessage replaceAtHeadMessage = (ReplaceAtHeadMessage)message;
        byte[] encodedExpectedChain = chainCodec.encode(replaceAtHeadMessage.getExpect());
        byte[] encodedUpdatedChain = chainCodec.encode(replaceAtHeadMessage.getUpdate());
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + MESSAGE_ID_SIZE + 2 * cacheIdLen +
                                         CHAIN_LEN_SIZE + encodedExpectedChain.length + encodedUpdatedChain.length);
        putCacheIdKeyAndOpCode(encodedMsg, replaceAtHeadMessage, replaceAtHeadMessage.getKey());
        encodedMsg.putInt(encodedExpectedChain.length);
        encodedMsg.put(encodedExpectedChain);
        encodedMsg.put(encodedUpdatedChain);
        return encodedMsg.array();
      case CLIENT_INVALIDATION_ACK:
        ClientInvalidationAck clientInvalidationAck = (ClientInvalidationAck)message;
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + INVALIDATION_ID_LEN_SIZE + 2 * cacheIdLen);
        encodedMsg.put(clientInvalidationAck.getOpCode());
        encodedMsg.putInt(clientInvalidationAck.getInvalidationId());
        CodecUtil.putStringAsCharArray(encodedMsg, clientInvalidationAck.getCacheId());
        return encodedMsg.array();
      case CLEAR:
        ClearMessage clearMessage = (ClearMessage)message;
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + MESSAGE_ID_SIZE + 2 * cacheIdLen);
        encodedMsg.put(clearMessage.getOpCode());
        encodedMsg.put(ClusteredEhcacheIdentity.serialize(message.getClientId()));
        encodedMsg.putLong(message.getId());
        CodecUtil.putStringAsCharArray(encodedMsg, clearMessage.getCacheId());
        return encodedMsg.array();
      default:
        throw new UnsupportedOperationException("This operation is not supported : " + message.operation());
    }
  }

  // This assumes correct allocation and puts extracts common code
  private static void putCacheIdKeyAndOpCode(ByteBuffer byteBuffer, ServerStoreOpMessage message, long key) {
    byteBuffer.put(message.getOpCode());
    byteBuffer.put(ClusteredEhcacheIdentity.serialize(message.getClientId()));
    byteBuffer.putLong(message.getId());
    byteBuffer.putInt(message.getCacheId().length());
    CodecUtil.putStringAsCharArray(byteBuffer, message.getCacheId());
    byteBuffer.putLong(key);
  }

  public EhcacheEntityMessage decode(byte[] payload) {
    ByteBuffer msg = ByteBuffer.wrap(payload);
    byte opCode = msg.get();
    ServerStoreOp storeOp = ServerStoreOp.getServerStoreOp(opCode);

    long key;
    String cacheId;
    UUID clientId;
    long msgId;

    EhcacheEntityMessage decodecMsg;
    switch (storeOp) {
      case GET:
        key = msg.getLong();
        cacheId = CodecUtil.getStringFromBuffer(msg, msg.remaining() / 2);
        return new GetMessage(cacheId, key);
      case GET_AND_APPEND:
        clientId = getClientId(msg);
        msgId = msg.getLong();
        cacheId = readStringFromBufferWithSize(msg);
        key = msg.getLong();
        decodecMsg = new GetAndAppendMessage(cacheId, key, msg.slice().asReadOnlyBuffer(), clientId);
        decodecMsg.setId(msgId);
        return decodecMsg;
      case APPEND:
        clientId = getClientId(msg);
        msgId = msg.getLong();
        cacheId = readStringFromBufferWithSize(msg);
        key = msg.getLong();
        decodecMsg = new AppendMessage(cacheId, key, msg.slice().asReadOnlyBuffer(), clientId);
        decodecMsg.setId(msgId);
        return decodecMsg;
      case REPLACE:
        clientId = getClientId(msg);
        msgId = msg.getLong();
        cacheId = readStringFromBufferWithSize(msg);
        key = msg.getLong();
        int expectChainLen = msg.getInt();
        byte[] encodedExpectChain = new byte[expectChainLen];
        msg.get(encodedExpectChain);
        int updateChainLen = msg.remaining();
        byte[] encodedUpdateChain = new byte[updateChainLen];
        msg.get(encodedUpdateChain);
        decodecMsg = new ReplaceAtHeadMessage(cacheId, key, chainCodec.decode(encodedExpectChain),
            chainCodec.decode(encodedUpdateChain), clientId);
        decodecMsg.setId(msgId);
        return decodecMsg;
      case CLIENT_INVALIDATION_ACK:
        int invalidationId = msg.getInt();
        cacheId = CodecUtil.getStringFromBuffer(msg, msg.remaining() / 2);
        return new ClientInvalidationAck(cacheId, invalidationId);
      case CLEAR:
        clientId = getClientId(msg);
        msgId = msg.getLong();
        cacheId = CodecUtil.getStringFromBuffer(msg, msg.remaining() / 2);
        decodecMsg = new ClearMessage(cacheId, clientId);
        decodecMsg.setId(msgId);
        return decodecMsg;
      default:
        throw new UnsupportedOperationException("This operation code is not supported : " + opCode);
    }
  }

  private static String readStringFromBufferWithSize(ByteBuffer buffer) {
    int length = buffer.getInt();
    return CodecUtil.getStringFromBuffer(buffer, length);
  }

  private static UUID getClientId(ByteBuffer payload) {
    long msb = payload.getLong();
    long lsb = payload.getLong();
    return new UUID(msb, lsb);
  }

}
