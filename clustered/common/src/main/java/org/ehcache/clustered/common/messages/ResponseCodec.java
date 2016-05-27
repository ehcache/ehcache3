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

import org.ehcache.clustered.common.store.Util;

import java.nio.ByteBuffer;

/**
 *
 */
class ResponseCodec {

  private static final byte OP_CODE_SIZE = 1;

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
        byte[] encodedChain = ChainCodec.encode(getResponse.getChain());
        int chainLen = encodedChain.length;
        buffer = ByteBuffer.allocate(OP_CODE_SIZE + chainLen);
        buffer.put(EhcacheEntityResponse.Type.GET_RESPONSE.getOpCode());
        buffer.put(encodedChain);
        return buffer.array();
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
        Exception exception = (Exception)Util.unmarshall(payArr);
        return new EhcacheEntityResponse.Failure(exception);
      case GET_RESPONSE:
        return new EhcacheEntityResponse.GetResponse(ChainCodec.decode(payArr));
      default:
        throw new UnsupportedOperationException("The operation is not supported with opCode : " + opCode);
    }
  }
}
