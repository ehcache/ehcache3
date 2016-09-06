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

import java.nio.ByteBuffer;
import java.util.UUID;

public class ReconnectDataCodec {

  public byte[] encode(ReconnectData reconnectData) {
    ByteBuffer encodedMsg = ByteBuffer.allocate(reconnectData.getDataLength());
    encodedMsg.put(ClusteredEhcacheIdentity.serialize(reconnectData.getClientId()));
    for (String cacheId : reconnectData.getAllCaches()) {
      encodedMsg.putInt(cacheId.length());
      CodecUtil.putStringAsCharArray(encodedMsg, cacheId);
    }

    return encodedMsg.array();
  }

  public ReconnectData decode(byte[] payload) {
    ReconnectData reconnectData = new ReconnectData();
    ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
    long msb = byteBuffer.getLong();
    long lsb = byteBuffer.getLong();
    reconnectData.setClientId(new UUID(msb, lsb));

    while (byteBuffer.hasRemaining()) {
      int cacheIdSize = byteBuffer.getInt();
      reconnectData.add(CodecUtil.getStringFromBuffer(byteBuffer, cacheIdSize));
    }
    return reconnectData;
  }

}
