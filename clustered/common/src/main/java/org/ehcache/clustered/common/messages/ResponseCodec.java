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

import static org.ehcache.clustered.common.messages.EhcacheEntityResponse.Failure;
import static org.ehcache.clustered.common.messages.EhcacheEntityResponse.GetResponse;

public class ResponseCodec {

  private static final byte OP_CODE_OFFSET = 1;

  public static byte[] encode(EhcacheEntityResponse response) {
    switch (response.getType()) {
      case FAILURE:
        Failure failure = (Failure)response;
        byte[] failureMsg = Util.marshall(failure.getCause());
        ByteBuffer buffer = ByteBuffer.allocate(OP_CODE_OFFSET + failureMsg.length);
        buffer.put(EhcacheEntityResponse.Type.FAILURE.getOpCode());
        buffer.put(failureMsg);
        return buffer.array();
      case SUCCESS:
        buffer = ByteBuffer.allocate(OP_CODE_OFFSET);
        buffer.put(EhcacheEntityResponse.Type.SUCCESS.getOpCode());
        return buffer.array();
      case GET_RESPONSE:
        GetResponse getResponse = (GetResponse)response;
        byte[] encodedChain = ChainCodec.encode(getResponse.getChain());
        int chainLen = encodedChain.length;
        buffer = ByteBuffer.allocate(OP_CODE_OFFSET + chainLen);
        buffer.put(EhcacheEntityResponse.Type.GET_RESPONSE.getOpCode());
        buffer.put(encodedChain);
        return buffer.array();
      default:
        throw new UnsupportedOperationException("The operation is not supported : " + response.getType());
    }
  }

  public static EhcacheEntityResponse decode(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    byte opCode = buffer.get();
    EhcacheEntityResponse.Type type = EhcacheEntityResponse.Type.responseType(opCode);
    byte[] payArr = new byte[buffer.remaining()];
    buffer.get(payArr);
    switch (type) {
      case SUCCESS:
        return EhcacheEntityResponse.success();
      case FAILURE:
        Exception exception = (Exception)Util.unmarshall(payArr);
        return EhcacheEntityResponse.failure(exception);
      case GET_RESPONSE:
        return EhcacheEntityResponse.response(ChainCodec.decode(payArr));
      default:
        throw new UnsupportedOperationException("The operation is not supported with opCode : " + opCode);
    }
  }
}
