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


import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static org.ehcache.clustered.common.messages.ServerStoreOpMessage.AppendMessage;
import static org.ehcache.clustered.common.messages.ServerStoreOpMessage.GetAndAppendMessage;
import static org.ehcache.clustered.common.messages.ServerStoreOpMessage.ReplaceAtHeadMessage;

/**
 *
 */
public class ServerStoreOpCodec {

  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private static final byte MSG_TYPE_OFFSET = 1;
  private static final byte STORE_OP_CODE_OFFSET = 1;
  private static final byte CACHE_ID_LEN_OFFSET = 4;
  private static final byte KEY_OFFSET = 8;
  private static final byte CHAIN_LEN_OFFSET = 4;

  public static byte[] encode(ServerStoreOpMessage message) {
    // TODO: improve data send over n/w by optimizing cache Id
    int cacheIdLen = message.getCacheId().length();
    switch (message.operation()) {
      case GET:
        ByteBuffer encodedMsg = ByteBuffer.allocate(MSG_TYPE_OFFSET + STORE_OP_CODE_OFFSET + CACHE_ID_LEN_OFFSET + KEY_OFFSET + cacheIdLen);
        encodedMsg.put(EhcacheEntityMessage.Type.SERVER_STORE_OP.getOpCode());
        encodedMsg.putInt(cacheIdLen);
        encodedMsg.put(message.getCacheId().getBytes(UTF_8));
        encodedMsg.putLong(message.getKey());
        encodedMsg.put(message.operation().getStoreOpCode());
        return encodedMsg.array();
      case APPEND:
        AppendMessage appendMessage = (AppendMessage)message;
        encodedMsg = ByteBuffer.allocate(MSG_TYPE_OFFSET + STORE_OP_CODE_OFFSET + CACHE_ID_LEN_OFFSET + KEY_OFFSET + cacheIdLen + appendMessage
            .getPayload()
            .remaining());
        encodedMsg.put(EhcacheEntityMessage.Type.SERVER_STORE_OP.getOpCode());
        encodedMsg.putInt(cacheIdLen);
        encodedMsg.put(message.getCacheId().getBytes(UTF_8));
        encodedMsg.putLong(message.getKey());
        encodedMsg.put(message.operation().getStoreOpCode());
        encodedMsg.put(appendMessage.getPayload());
        return encodedMsg.array();
      case GET_AND_APPEND:
        GetAndAppendMessage getAndAppendMessage = (GetAndAppendMessage)message;
        encodedMsg = ByteBuffer.allocate(MSG_TYPE_OFFSET + STORE_OP_CODE_OFFSET + CACHE_ID_LEN_OFFSET + KEY_OFFSET + cacheIdLen + getAndAppendMessage
            .getPayload()
            .remaining());
        encodedMsg.put(EhcacheEntityMessage.Type.SERVER_STORE_OP.getOpCode());
        encodedMsg.putInt(cacheIdLen);
        encodedMsg.put(message.getCacheId().getBytes(UTF_8));
        encodedMsg.putLong(message.getKey());
        encodedMsg.put(message.operation().getStoreOpCode());
        encodedMsg.put(getAndAppendMessage.getPayload());
        return encodedMsg.array();
      case REPLACE:
        ReplaceAtHeadMessage replaceAtHeadMessage = (ReplaceAtHeadMessage)message;
        byte[] encodedExpectedChain = ChainCodec.encode(replaceAtHeadMessage.getExpect());
        byte[] encodedUpdatedChain = ChainCodec.encode(replaceAtHeadMessage.getUpdate());
        encodedMsg = ByteBuffer.allocate(MSG_TYPE_OFFSET + STORE_OP_CODE_OFFSET + CACHE_ID_LEN_OFFSET + KEY_OFFSET + cacheIdLen +
              2 * CHAIN_LEN_OFFSET + encodedExpectedChain.length + encodedUpdatedChain.length);
        encodedMsg.put(EhcacheEntityMessage.Type.SERVER_STORE_OP.getOpCode());
        encodedMsg.putInt(cacheIdLen);
        encodedMsg.put(message.getCacheId().getBytes(UTF_8));
        encodedMsg.putLong(message.getKey());
        encodedMsg.put(message.operation().getStoreOpCode());
        encodedMsg.putInt(encodedExpectedChain.length);
        encodedMsg.put(encodedExpectedChain);
        encodedMsg.putInt(encodedUpdatedChain.length);
        encodedMsg.put(encodedUpdatedChain);
        return encodedMsg.array();
      default:
        throw new UnsupportedOperationException("This operation is not supported : " + message.operation());
    }
  }

  public static EhcacheEntityMessage decode(byte[] payload) {
    ByteBuffer msg = ByteBuffer.wrap(payload);
    msg.get();
    int cacheIdLen = msg.getInt();
    byte[] idArr = new byte[cacheIdLen];
    msg.get(idArr);
    String cacheId = new String(idArr, UTF_8);
    long key = msg.getLong();
    byte opCode = msg.get();
    ServerStoreOpMessage.ServerStoreOp storeOp = ServerStoreOpMessage.ServerStoreOp.getServerStoreOp(opCode);
    byte[] remaining = new byte[msg.remaining()];
    msg.get(remaining);
    switch (storeOp) {
      case GET:
        return EhcacheEntityMessage.getOperation(cacheId, key);
      case GET_AND_APPEND:
        return EhcacheEntityMessage.getAndAppendOperation(cacheId, key, ByteBuffer.wrap(remaining).asReadOnlyBuffer());
      case APPEND:
        return EhcacheEntityMessage.appendOperation(cacheId, key, ByteBuffer.wrap(remaining).asReadOnlyBuffer());
      case REPLACE:
        ByteBuffer replaceBuf = ByteBuffer.wrap(remaining);
        int expectChainLen = replaceBuf.getInt();
        byte[] encodedExpectChain = new byte[expectChainLen];
        replaceBuf.get(encodedExpectChain);
        int updateChainLen = replaceBuf.getInt();
        byte[] encodedUpdateChain = new byte[updateChainLen];
        replaceBuf.get(encodedUpdateChain);
        return EhcacheEntityMessage.replaceAtHeadOperation(cacheId, key, ChainCodec.decode(encodedExpectChain),
            ChainCodec.decode(encodedUpdateChain));
      default:
        throw new UnsupportedOperationException("This operation code is not supported : " + opCode);

    }
  }

}