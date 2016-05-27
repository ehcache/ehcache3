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
import org.ehcache.clustered.common.messages.ServerStoreOpMessage.GetAndAppendMessage;
import org.ehcache.clustered.common.messages.ServerStoreOpMessage.GetMessage;
import org.ehcache.clustered.common.messages.ServerStoreOpMessage.ReplaceAtHeadMessage;
import org.ehcache.clustered.common.messages.ServerStoreOpMessage.ServerStoreOp;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 *
 */
class ServerStoreOpCodec {


  private static final byte MSG_TYPE_SIZE = 1;
  private static final byte STORE_OP_CODE_SIZE = 1;
  private static final byte CACHE_ID_LEN_SIZE = 4;
  private static final byte KEY_SIZE = 8;
  private static final byte CHAIN_LEN_SIZE = 4;

  public byte[] encode(ServerStoreOpMessage message) {
    // TODO: improve data send over n/w by optimizing cache Id
    ByteBuffer encodedMsg;
    int cacheIdLen = message.getCacheId().length();
    switch (message.operation()) {
      case GET:
        encodedMsg = ByteBuffer.allocate(MSG_TYPE_SIZE + STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + 2 * cacheIdLen);
        putCacheIdKeyAndOpCode(encodedMsg, message.getCacheId(), message.getKey(), message.operation().getStoreOpCode());
        return encodedMsg.array();
      case APPEND:
        AppendMessage appendMessage = (AppendMessage)message;
        encodedMsg = ByteBuffer.allocate(MSG_TYPE_SIZE + STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + 2 * cacheIdLen + appendMessage
            .getPayload()
            .remaining());
        putCacheIdKeyAndOpCode(encodedMsg, message.getCacheId(), message.getKey(), message.operation().getStoreOpCode());
        encodedMsg.put(appendMessage.getPayload());
        return encodedMsg.array();
      case GET_AND_APPEND:
        GetAndAppendMessage getAndAppendMessage = (GetAndAppendMessage)message;
        encodedMsg = ByteBuffer.allocate(MSG_TYPE_SIZE + STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + 2 * cacheIdLen + getAndAppendMessage
            .getPayload()
            .remaining());
        putCacheIdKeyAndOpCode(encodedMsg, message.getCacheId(), message.getKey(), message.operation().getStoreOpCode());
        encodedMsg.put(getAndAppendMessage.getPayload());
        return encodedMsg.array();
      case REPLACE:
        ReplaceAtHeadMessage replaceAtHeadMessage = (ReplaceAtHeadMessage)message;
        byte[] encodedExpectedChain = ChainCodec.encode(replaceAtHeadMessage.getExpect());
        byte[] encodedUpdatedChain = ChainCodec.encode(replaceAtHeadMessage.getUpdate());
        encodedMsg = ByteBuffer.allocate(MSG_TYPE_SIZE + STORE_OP_CODE_SIZE + CACHE_ID_LEN_SIZE + KEY_SIZE + 2 * cacheIdLen +
                                         2 * CHAIN_LEN_SIZE + encodedExpectedChain.length + encodedUpdatedChain.length);
        putCacheIdKeyAndOpCode(encodedMsg, message.getCacheId(), message.getKey(), message.operation().getStoreOpCode());
        encodedMsg.putInt(encodedExpectedChain.length);
        encodedMsg.put(encodedExpectedChain);
        encodedMsg.putInt(encodedUpdatedChain.length);
        encodedMsg.put(encodedUpdatedChain);
        return encodedMsg.array();
      default:
        throw new UnsupportedOperationException("This operation is not supported : " + message.operation());
    }
  }

  // This assumes correct allocation and puts extracts common code
  private static void putCacheIdKeyAndOpCode(ByteBuffer byteBuffer, String cacheId, long key, byte opcode) {
    byteBuffer.put(EhcacheEntityMessage.Type.SERVER_STORE_OP.getOpCode());
    byteBuffer.put(opcode);
    byteBuffer.putInt(cacheId.length());
    for (int i = 0; i < cacheId.length(); i++) {
      byteBuffer.putChar(cacheId.charAt(i));
    }
    byteBuffer.putLong(key);
  }

  public EhcacheEntityMessage decode(byte[] payload) {
    ByteBuffer msg = ByteBuffer.wrap(payload);
    msg.get();
    byte opCode = msg.get();
    ServerStoreOp storeOp = ServerStoreOp.getServerStoreOp(opCode);
    int cacheIdLen = msg.getInt();
    char[] idArr = new char[cacheIdLen];
    for (int i = 0; i < cacheIdLen; i++) {
      idArr[i] = msg.getChar();
    }
    String cacheId = new String(idArr);
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
        return new ReplaceAtHeadMessage(cacheId, key, ChainCodec.decode(encodedExpectChain),
            ChainCodec.decode(encodedUpdateChain));
      default:
        throw new UnsupportedOperationException("This operation code is not supported : " + opCode);

    }
  }

}
