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

import org.ehcache.clustered.common.messages.ServerStoreOpMessage.AppendMessage;
import org.ehcache.clustered.common.messages.ServerStoreOpMessage.ClearMessage;
import org.ehcache.clustered.common.messages.ServerStoreOpMessage.GetAndAppendMessage;
import org.ehcache.clustered.common.messages.ServerStoreOpMessage.GetMessage;
import org.ehcache.clustered.common.messages.ServerStoreOpMessage.ReplaceAtHeadMessage;
import org.ehcache.clustered.common.messages.ServerStoreOpMessage.ClientInvalidationAck;
import org.ehcache.clustered.common.messages.ServerStoreOpMessage.ServerStoreOp;

import java.nio.ByteBuffer;

/**
 *
 */
class ServerStoreOpCodec {


  private static final byte STORE_OP_CODE_SIZE = 1;
  private static final byte CACHE_ID_LEN_SIZE = 4;
  private static final byte KEY_SIZE = 8;
  private static final byte CHAIN_LEN_SIZE = 4;

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
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + 2 * cacheIdLen);
        putCacheIdKeyAndOpCode(encodedMsg, message.getCacheId(), message.getKey(), message.operation().getStoreOpCode());
        return encodedMsg.array();
      case APPEND:
        AppendMessage appendMessage = (AppendMessage)message;
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + 2 * cacheIdLen + appendMessage
            .getPayload()
            .remaining());
        putCacheIdKeyAndOpCode(encodedMsg, message.getCacheId(), message.getKey(), message.operation().getStoreOpCode());
        encodedMsg.put(appendMessage.getPayload());
        return encodedMsg.array();
      case GET_AND_APPEND:
        GetAndAppendMessage getAndAppendMessage = (GetAndAppendMessage)message;
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + 2 * cacheIdLen + getAndAppendMessage
            .getPayload()
            .remaining());
        putCacheIdKeyAndOpCode(encodedMsg, message.getCacheId(), message.getKey(), message.operation().getStoreOpCode());
        encodedMsg.put(getAndAppendMessage.getPayload());
        return encodedMsg.array();
      case REPLACE:
        ReplaceAtHeadMessage replaceAtHeadMessage = (ReplaceAtHeadMessage)message;
        byte[] encodedExpectedChain = chainCodec.encode(replaceAtHeadMessage.getExpect());
        byte[] encodedUpdatedChain = chainCodec.encode(replaceAtHeadMessage.getUpdate());
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + 2 * cacheIdLen +
                                         2 * CHAIN_LEN_SIZE + encodedExpectedChain.length + encodedUpdatedChain.length);
        putCacheIdKeyAndOpCode(encodedMsg, message.getCacheId(), message.getKey(), message.operation().getStoreOpCode());
        encodedMsg.putInt(encodedExpectedChain.length);
        encodedMsg.put(encodedExpectedChain);
        encodedMsg.putInt(encodedUpdatedChain.length);
        encodedMsg.put(encodedUpdatedChain);
        return encodedMsg.array();
      case CLIENT_INVALIDATION_ACK:
        ClientInvalidationAck clientInvalidationAck = (ClientInvalidationAck) message;
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + 2 * cacheIdLen + 4);
        putCacheIdKeyAndOpCode(encodedMsg, message.getCacheId(), 0L, message.operation().getStoreOpCode());
        encodedMsg.putInt(clientInvalidationAck.getInvalidationId());
        return encodedMsg.array();
      case CLEAR:
        ClearMessage clearMessage = (ClearMessage)message;
        encodedMsg = ByteBuffer.allocate(STORE_OP_CODE_SIZE + 2 * cacheIdLen);
        encodedMsg.put(clearMessage.getOpCode());
        putCacheId(encodedMsg, clearMessage.getCacheId());
        return encodedMsg.array();
      default:
        throw new UnsupportedOperationException("This operation is not supported : " + message.operation());
    }
  }

  // This assumes correct allocation and puts extracts common code
  private static void putCacheIdKeyAndOpCode(ByteBuffer byteBuffer, String cacheId, long key, byte opcode) {
    byteBuffer.put(opcode);
    byteBuffer.putInt(cacheId.length());
    putCacheId(byteBuffer, cacheId);
    byteBuffer.putLong(key);
  }

  private static void putCacheId(ByteBuffer byteBuffer, String cacheId) {
    for (int i = 0; i < cacheId.length(); i++) {
      byteBuffer.putChar(cacheId.charAt(i));
    }
  }

  public EhcacheEntityMessage decode(byte[] payload) {
    ByteBuffer msg = ByteBuffer.wrap(payload);
    byte opCode = msg.get();
    ServerStoreOp storeOp = ServerStoreOp.getServerStoreOp(opCode);
    if(storeOp == ServerStoreOp.CLEAR) {
      return new ClearMessage(getStringFromBuffer(msg, msg.remaining()/2));
    }

    int cacheIdLen = msg.getInt();
    String cacheId = getStringFromBuffer(msg, cacheIdLen);
    long key = msg.getLong();
    byte[] remaining = new byte[msg.remaining()];
    msg.get(remaining);
    switch (storeOp) {
      case GET:
        return new GetMessage(cacheId, key);
      case GET_AND_APPEND:
        return new GetAndAppendMessage(cacheId, key, ByteBuffer.wrap(remaining).asReadOnlyBuffer());
      case APPEND:
        return new AppendMessage(cacheId, key, ByteBuffer.wrap(remaining).asReadOnlyBuffer());
      case REPLACE:
        ByteBuffer replaceBuf = ByteBuffer.wrap(remaining);
        int expectChainLen = replaceBuf.getInt();
        byte[] encodedExpectChain = new byte[expectChainLen];
        replaceBuf.get(encodedExpectChain);
        int updateChainLen = replaceBuf.getInt();
        byte[] encodedUpdateChain = new byte[updateChainLen];
        replaceBuf.get(encodedUpdateChain);
        return new ReplaceAtHeadMessage(cacheId, key, chainCodec.decode(encodedExpectChain),
            chainCodec.decode(encodedUpdateChain));
      case CLIENT_INVALIDATION_ACK:
        ByteBuffer remainingBuf = ByteBuffer.wrap(remaining);
        int invalidationId = remainingBuf.getInt();
        return new ClientInvalidationAck(cacheId, invalidationId);
      default:
        throw new UnsupportedOperationException("This operation code is not supported : " + opCode);

    }
  }

  private static String getStringFromBuffer(ByteBuffer buffer, int length) {
    char[] arr = new char[length];
    for (int i = 0; i < length; i++) {
      arr[i] = buffer.getChar();
    }
    return new String(arr);
  }
}
