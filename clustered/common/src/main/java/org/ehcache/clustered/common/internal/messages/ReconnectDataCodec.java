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


import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class ReconnectDataCodec {

  private static final byte ENTRY_SIZE = 4;

  public byte[] encode(Set<String> cacheIds, int length) {
    ByteBuffer reconnectData = ByteBuffer.allocate(2 * length + cacheIds.size() * ENTRY_SIZE);
    for (String cacheId : cacheIds) {
      reconnectData.putInt(cacheId.length());
      CodecUtil.putStringAsCharArray(reconnectData, cacheId);
    }

    return reconnectData.array();
  }

  public Set<String> decode(byte[] payload) {
    Set<String> cacheIds = new HashSet<String>();
    ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
    while (byteBuffer.hasRemaining()) {
      int cacheIdSize = byteBuffer.getInt();
      cacheIds.add(CodecUtil.getStringFromBuffer(byteBuffer, cacheIdSize));
    }
    return cacheIds;
  }

}
