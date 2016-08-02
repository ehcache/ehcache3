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

import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.store.Util;

import java.nio.ByteBuffer;

import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.AllInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.ClientInvalidateAll;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.ClientInvalidateHash;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.HashInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.ServerInvalidateHash;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.MapValue;

class ResponseCodec {

  private static final byte OP_CODE_SIZE = 1;

  private final ChainCodec chainCodec;

  ResponseCodec() {
    this.chainCodec = new ChainCodec();
  }

  public byte[] encode(EhcacheEntityResponse response) {
    switch (response.getType()) {
      case FAILURE:
        EhcacheEntityResponse.Failure failure = (EhcacheEntityResponse.Failure)response;
        byte[] failureMsg = Util.marshall(failure.getCause());
        ByteBuffer buffer = ByteBuffer.allocate(OP_CODE_SIZE + failureMsg.length);
        buffer.put(EhcacheEntityResponse.Type.FAILURE.getOpCode());
        buffer.put(failureMsg);
        return buffer.array();
      case SUCCESS:
        buffer = ByteBuffer.allocate(OP_CODE_SIZE);
        buffer.put(EhcacheEntityResponse.Type.SUCCESS.getOpCode());
        return buffer.array();
      case GET_RESPONSE:
        EhcacheEntityResponse.GetResponse getResponse = (EhcacheEntityResponse.GetResponse)response;
        byte[] encodedChain = chainCodec.encode(getResponse.getChain());
        int chainLen = encodedChain.length;
        buffer = ByteBuffer.allocate(OP_CODE_SIZE + chainLen);
        buffer.put(EhcacheEntityResponse.Type.GET_RESPONSE.getOpCode());
        buffer.put(encodedChain);
        return buffer.array();
      case HASH_INVALIDATION_DONE: {
        HashInvalidationDone hashInvalidationDone = (HashInvalidationDone) response;
        buffer = ByteBuffer.allocate(OP_CODE_SIZE + hashInvalidationDone.getCacheId().length() * 2 + 8);
        buffer.put(EhcacheEntityResponse.Type.HASH_INVALIDATION_DONE.getOpCode());
        CodecUtil.putStringAsCharArray(buffer, hashInvalidationDone.getCacheId());
        buffer.putLong(hashInvalidationDone.getKey());
        return buffer.array();
      }
      case ALL_INVALIDATION_DONE: {
        AllInvalidationDone allInvalidationDone = (AllInvalidationDone) response;
        buffer = ByteBuffer.allocate(OP_CODE_SIZE + allInvalidationDone.getCacheId().length() * 2);
        buffer.put(EhcacheEntityResponse.Type.ALL_INVALIDATION_DONE.getOpCode());
        CodecUtil.putStringAsCharArray(buffer, allInvalidationDone.getCacheId());
        return buffer.array();
      }
      case CLIENT_INVALIDATE_HASH: {
        ClientInvalidateHash clientInvalidateHash = (ClientInvalidateHash) response;
        buffer = ByteBuffer.allocate(OP_CODE_SIZE + clientInvalidateHash.getCacheId().length() * 2 + 12);
        buffer.put(EhcacheEntityResponse.Type.CLIENT_INVALIDATE_HASH.getOpCode());
        CodecUtil.putStringAsCharArray(buffer, clientInvalidateHash.getCacheId());
        buffer.putLong(clientInvalidateHash.getKey());
        buffer.putInt(((ClientInvalidateHash) response).getInvalidationId());
        return buffer.array();
      }
      case CLIENT_INVALIDATE_ALL: {
        ClientInvalidateAll clientInvalidateAll = (ClientInvalidateAll) response;
        buffer = ByteBuffer.allocate(OP_CODE_SIZE + clientInvalidateAll.getCacheId().length() * 2 + 4);
        buffer.put(EhcacheEntityResponse.Type.CLIENT_INVALIDATE_ALL.getOpCode());
        CodecUtil.putStringAsCharArray(buffer, clientInvalidateAll.getCacheId());
        buffer.putInt(((ClientInvalidateAll) response).getInvalidationId());
        return buffer.array();
      }
      case SERVER_INVALIDATE_HASH: {
        ServerInvalidateHash serverInvalidateHash = (ServerInvalidateHash) response;
        buffer = ByteBuffer.allocate(OP_CODE_SIZE + serverInvalidateHash.getCacheId().length() * 2 + 8);
        buffer.put(EhcacheEntityResponse.Type.SERVER_INVALIDATE_HASH.getOpCode());
        CodecUtil.putStringAsCharArray(buffer, serverInvalidateHash.getCacheId());
        buffer.putLong(serverInvalidateHash.getKey());
        return buffer.array();
      }
      case MAP_VALUE: {
        MapValue mapValue = (MapValue) response;
        byte[] encodedMapValue = Util.marshall(mapValue.getValue());
        buffer = ByteBuffer.allocate(OP_CODE_SIZE + encodedMapValue.length);
        buffer.put(EhcacheEntityResponse.Type.MAP_VALUE.getOpCode());
        buffer.put(encodedMapValue);
        return buffer.array();
      }
      default:
        throw new UnsupportedOperationException("The operation is not supported : " + response.getType());
    }
  }

  public EhcacheEntityResponse decode(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    byte opCode = buffer.get();
    EhcacheEntityResponse.Type type = EhcacheEntityResponse.Type.responseType(opCode);
    byte[] payArr = new byte[buffer.remaining()];
    buffer.get(payArr);
    switch (type) {
      case SUCCESS:
        return EhcacheEntityResponse.Success.INSTANCE;
      case FAILURE:
        ClusterException exception = (ClusterException)Util.unmarshall(payArr);
        return new EhcacheEntityResponse.Failure(exception.withClientStackTrace());
      case GET_RESPONSE:
        return new EhcacheEntityResponse.GetResponse(chainCodec.decode(payArr));
      case HASH_INVALIDATION_DONE: {
        String cacheId = ByteBuffer.wrap(payArr, 0, payArr.length - 8).asCharBuffer().toString();
        long key = ByteBuffer.wrap(payArr, payArr.length - 8, 8).getLong();
        return EhcacheEntityResponse.hashInvalidationDone(cacheId, key);
      }
      case ALL_INVALIDATION_DONE: {
        String cacheId = ByteBuffer.wrap(payArr).asCharBuffer().toString();
        return EhcacheEntityResponse.allInvalidationDone(cacheId);
      }
      case CLIENT_INVALIDATE_HASH: {
        String cacheId = ByteBuffer.wrap(payArr, 0, payArr.length - 12).asCharBuffer().toString();
        ByteBuffer byteBuffer = ByteBuffer.wrap(payArr, payArr.length - 12, 12);
        long key = byteBuffer.getLong();
        int invalidationId = byteBuffer.getInt();
        return EhcacheEntityResponse.clientInvalidateHash(cacheId, key, invalidationId);
      }
      case CLIENT_INVALIDATE_ALL: {
        String cacheId = ByteBuffer.wrap(payArr, 0, payArr.length - 4).asCharBuffer().toString();
        ByteBuffer byteBuffer = ByteBuffer.wrap(payArr, payArr.length - 4, 4);
        int invalidationId = byteBuffer.getInt();
        return EhcacheEntityResponse.clientInvalidateAll(cacheId, invalidationId);
      }
      case SERVER_INVALIDATE_HASH: {
        String cacheId = ByteBuffer.wrap(payArr, 0, payArr.length - 8).asCharBuffer().toString();
        ByteBuffer byteBuffer = ByteBuffer.wrap(payArr, payArr.length - 8, 8);
        long key = byteBuffer.getLong();
        return EhcacheEntityResponse.serverInvalidateHash(cacheId, key);
      }
      case MAP_VALUE: {
        return EhcacheEntityResponse.mapValue(Util.unmarshall(payArr));
      }
      default:
        throw new UnsupportedOperationException("The operation is not supported with opCode : " + type);
    }
  }
}
