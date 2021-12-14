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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ReconnectMessageCodec {

  private static final byte CLIENT_ID_SIZE = 16;
  private static final byte ENTRY_SIZE = 4;
  private static final byte HASH_SIZE = 8;
  private static final byte CLEAR_IN_PROGRESS_STATUS_SIZE = 1;

  public byte[] encode(ReconnectMessage reconnectMessage) {
    int totalLength = 0;
    Set<String> caches = reconnectMessage.getAllCaches();
    List<ByteBuffer> byteBuffers = new ArrayList<ByteBuffer>();
    for (String cache : caches) {
      Set<Long> hashToInvalidate = reconnectMessage.getInvalidationsInProgress(cache);
      int sizeOfBuffer = 2 * cache.length() + CLEAR_IN_PROGRESS_STATUS_SIZE + hashToInvalidate.size() * HASH_SIZE + 2 * ENTRY_SIZE;
      ByteBuffer encodedCache = ByteBuffer.allocate(sizeOfBuffer);
      encodedCache.putInt(cache.length());
      CodecUtil.putStringAsCharArray(encodedCache, cache);
      if (reconnectMessage.isClearInProgress(cache)) {
        encodedCache.put((byte)1);
      } else {
        encodedCache.put((byte)0);
      }
      encodedCache.putInt(hashToInvalidate.size());
      for (long hash : hashToInvalidate) {
        encodedCache.putLong(hash);
      }
      encodedCache.flip();
      byteBuffers.add(encodedCache);
      totalLength += sizeOfBuffer;
    }
    ByteBuffer encodedMsg = ByteBuffer.allocate(totalLength + CLIENT_ID_SIZE);
    encodedMsg.put(ClusteredEhcacheIdentity.serialize(reconnectMessage.getClientId()));
    for (ByteBuffer byteBuffer : byteBuffers) {
      encodedMsg.put(byteBuffer);
    }
    return encodedMsg.array();
  }

  public ReconnectMessage decode(byte[] payload) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
    long msb = byteBuffer.getLong();
    long lsb = byteBuffer.getLong();

    Map<String, Set<Long>> caches = new HashMap<String, Set<Long>>();
    Set<String> clearInProgressCache = new HashSet<String>();

    while (byteBuffer.hasRemaining()) {
      int cacheIdSize = byteBuffer.getInt();
      String cacheId = CodecUtil.getStringFromBuffer(byteBuffer, cacheIdSize);
      byte clearInProgress = byteBuffer.get();
      if (clearInProgress == 1) {
        clearInProgressCache.add(cacheId);
      }
      Set<Long> hashToInvalidate = new HashSet<Long>();
      int numOfHash = byteBuffer.getInt();
      for (int i = 0; i < numOfHash; i++) {
        hashToInvalidate.add(byteBuffer.getLong());
      }
      caches.put(cacheId, hashToInvalidate);
    }
    ReconnectMessage reconnectMessage = new ReconnectMessage(new UUID(msb, lsb), caches.keySet());
    for (Map.Entry<String, Set<Long>> cacheEntry : caches.entrySet()) {
      if (clearInProgressCache.contains(cacheEntry.getKey())) {
        reconnectMessage.addClearInProgress(cacheEntry.getKey());
      }
      reconnectMessage.addInvalidationsInProgress(cacheEntry.getKey(), cacheEntry.getValue());
    }
    return reconnectMessage;
  }
}
