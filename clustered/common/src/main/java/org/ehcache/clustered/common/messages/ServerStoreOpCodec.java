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

import static org.ehcache.clustered.common.messages.ServerStoreOpMessage.AppendMessage;
import static org.ehcache.clustered.common.messages.ServerStoreOpMessage.GetAndAppendMessage;

/**
 *
 */
public class ServerStoreOpCodec {

  private static final byte MSG_TYPE_OFFSET = 1;
  private static final byte STORE_OP_CODE_OFFSET = 1;
  private static final int CACHE_ID_LEN_OFFSET = 4;
  private static final int KEY_OFFSET = 16;

  public static byte[] encode(ServerStoreOpMessage message) {
    // TODO: improve data send over n/w by optimizing cache Id
    int cacheIdLen = message.getCacheId().length();
    switch (message.operation()) {
      case GET:
        ByteBuffer encodedMsg = ByteBuffer.allocate(MSG_TYPE_OFFSET + STORE_OP_CODE_OFFSET + CACHE_ID_LEN_OFFSET + KEY_OFFSET + cacheIdLen);
        encodedMsg.put(EhcacheEntityMessage.Type.SERVER_STORE_OP.getOpCode());
        encodedMsg.putInt(cacheIdLen);
        encodedMsg.put(message.getCacheId().getBytes());
        encodedMsg.put(message.operation().getStoreOpCode());
        encodedMsg.putLong(message.getKey());
        encodedMsg.flip();
        return encodedMsg.array();
      case APPEND:
        AppendMessage appendMessage = (AppendMessage)message;
        encodedMsg = ByteBuffer.allocate(MSG_TYPE_OFFSET + STORE_OP_CODE_OFFSET + CACHE_ID_LEN_OFFSET + KEY_OFFSET + cacheIdLen + appendMessage
            .getPayload()
            .remaining());
        encodedMsg.put(EhcacheEntityMessage.Type.SERVER_STORE_OP.getOpCode());
        encodedMsg.putInt(cacheIdLen);
        encodedMsg.put(message.getCacheId().getBytes());
        encodedMsg.put(message.operation().getStoreOpCode());
        encodedMsg.putLong(message.getKey());
        encodedMsg.put(appendMessage.getPayload());
        encodedMsg.flip();
        return encodedMsg.array();
      case GETANDAPPEND:
        GetAndAppendMessage getAndAppendMessage = (GetAndAppendMessage)message;
        encodedMsg = ByteBuffer.allocate(MSG_TYPE_OFFSET + STORE_OP_CODE_OFFSET + CACHE_ID_LEN_OFFSET + KEY_OFFSET + cacheIdLen + getAndAppendMessage
            .getPayload()
            .remaining());
        encodedMsg.put(EhcacheEntityMessage.Type.SERVER_STORE_OP.getOpCode());
        encodedMsg.putInt(cacheIdLen);
        encodedMsg.put(message.getCacheId().getBytes());
        encodedMsg.put(message.operation().getStoreOpCode());
        encodedMsg.putLong(message.getKey());
        encodedMsg.put(getAndAppendMessage.getPayload());
        encodedMsg.flip();
        return encodedMsg.array();
      case REPLACE:
        //TODO: do this
      default:
        throw new UnsupportedOperationException("This operation is not supported" + message.operation());
    }
  }

  public static EhcacheEntityMessage decode(byte[] payload) {
    ByteBuffer msg = ByteBuffer.wrap(payload);
    msg.get();
    int cacheIdLen = msg.getInt();
    byte[] idArr = new byte[cacheIdLen];
    msg.get(idArr);
    String cacheId = new String(idArr);
    long key = msg.getLong();
    byte opCode = msg.get();
    switch (opCode) {
      case 0:
        return EhcacheEntityMessage.getOperation(cacheId, key);
      case 1:
        return EhcacheEntityMessage.appendOperation(cacheId, key, msg.compact());
      case 2:
        return EhcacheEntityMessage.getAndAppendOperation(cacheId, key, msg.compact());
      case 3:
        //TODO: do this
      default:
        throw new UnsupportedOperationException("This operation code is not supported" + opCode);

    }
  }

}