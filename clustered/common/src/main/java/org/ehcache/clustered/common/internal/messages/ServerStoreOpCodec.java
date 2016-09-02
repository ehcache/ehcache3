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

import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.AppendMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ClearMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.GetAndAppendMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.GetMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ReplaceAtHeadMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ClientInvalidationAck;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ServerStoreOp;

import java.nio.ByteBuffer;

class ServerStoreOpCodec {

  private static final byte STORE_OP_CODE_SIZE = 1;
  private static final byte CACHE_ID_LEN_SIZE = 4;
  private static final byte KEY_SIZE = 8;
  private static final byte CHAIN_LEN_SIZE = 4;
  private static final byte INVALIDATION_ID_LEN_SIZE = 4;

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
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + 2 * cacheIdLen + appendMessage
            .getPayload()
            .remaining());
        putCacheIdKeyAndOpCode(encodedMsg, appendMessage.getCacheId(), appendMessage.getKey(), appendMessage.getOpCode());
        encodedMsg.put(appendMessage.getPayload());
        return encodedMsg.array();
      case GET_AND_APPEND:
        GetAndAppendMessage getAndAppendMessage = (GetAndAppendMessage)message;
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + 2 * cacheIdLen +
                                         getAndAppendMessage.getPayload().remaining());
        putCacheIdKeyAndOpCode(encodedMsg, getAndAppendMessage.getCacheId(), getAndAppendMessage.getKey(), getAndAppendMessage.getOpCode());
        encodedMsg.put(getAndAppendMessage.getPayload());
        return encodedMsg.array();
      case REPLACE:
        ReplaceAtHeadMessage replaceAtHeadMessage = (ReplaceAtHeadMessage)message;
        byte[] encodedExpectedChain = chainCodec.encode(replaceAtHeadMessage.getExpect());
        byte[] encodedUpdatedChain = chainCodec.encode(replaceAtHeadMessage.getUpdate());
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + 2 * cacheIdLen +
                                         CHAIN_LEN_SIZE + encodedExpectedChain.length + encodedUpdatedChain.length);
        putCacheIdKeyAndOpCode(encodedMsg, replaceAtHeadMessage.getCacheId(), replaceAtHeadMessage.getKey(), replaceAtHeadMessage.getOpCode());
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
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + 2 * cacheIdLen);
        encodedMsg.put(clearMessage.getOpCode());
        CodecUtil.putStringAsCharArray(encodedMsg, clearMessage.getCacheId());
        return encodedMsg.array();
      default:
        throw new UnsupportedOperationException("This operation is not supported : " + message.operation());
    }
  }

  // This assumes correct allocation and puts extracts common code
  private static void putCacheIdKeyAndOpCode(ByteBuffer byteBuffer, String cacheId, long key, byte opcode) {
    byteBuffer.put(opcode);
    byteBuffer.putInt(cacheId.length());
    CodecUtil.putStringAsCharArray(byteBuffer, cacheId);
    byteBuffer.putLong(key);
  }

  public EhcacheEntityMessage decode(byte[] payload) {
    ByteBuffer msg = ByteBuffer.wrap(payload);
    byte opCode = msg.get();
    ServerStoreOp storeOp = ServerStoreOp.getServerStoreOp(opCode);

    long key;
    String cacheId;

    switch (storeOp) {
      case GET:
        key = msg.getLong();
        cacheId = CodecUtil.getStringFromBuffer(msg, msg.remaining() / 2);
        return new GetMessage(cacheId, key);
      case GET_AND_APPEND:
        cacheId = readStringFromBufferWithSize(msg);
        key = msg.getLong();
        return new GetAndAppendMessage(cacheId, key, msg.slice().asReadOnlyBuffer());
      case APPEND:
        cacheId = readStringFromBufferWithSize(msg);
        key = msg.getLong();
        return new AppendMessage(cacheId, key, msg.slice().asReadOnlyBuffer());
      case REPLACE:
        cacheId = readStringFromBufferWithSize(msg);
        key = msg.getLong();
        int expectChainLen = msg.getInt();
        byte[] encodedExpectChain = new byte[expectChainLen];
        msg.get(encodedExpectChain);
        int updateChainLen = msg.remaining();
        byte[] encodedUpdateChain = new byte[updateChainLen];
        msg.get(encodedUpdateChain);
        return new ReplaceAtHeadMessage(cacheId, key, chainCodec.decode(encodedExpectChain),
            chainCodec.decode(encodedUpdateChain));
      case CLIENT_INVALIDATION_ACK:
        int invalidationId = msg.getInt();
        cacheId = CodecUtil.getStringFromBuffer(msg, msg.remaining() / 2);
        return new ClientInvalidationAck(cacheId, invalidationId);
      case CLEAR:
        cacheId = CodecUtil.getStringFromBuffer(msg, msg.remaining() / 2);
        return new ClearMessage(cacheId);
      default:
        throw new UnsupportedOperationException("This operation code is not supported : " + opCode);
    }
  }

  private static String readStringFromBufferWithSize(ByteBuffer buffer) {
    int length = buffer.getInt();
    return CodecUtil.getStringFromBuffer(buffer, length);
  }

}
