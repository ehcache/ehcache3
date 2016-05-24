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


import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.Util;
import org.terracotta.entity.EntityResponse;

import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public abstract class EhcacheEntityResponse implements EntityResponse {

  private static final byte OP_CODE_SIZE = 1;

  public enum Type {
    SUCCESS((byte) 0),
    FAILURE((byte) 1),
    GET_RESPONSE((byte) 2);

    private final byte opCode;

    Type(byte opCode) {
      this.opCode = opCode;
    }

    public byte getOpCode() {
      return this.opCode;
    }

    public static Type responseType(byte opCode) {
      switch (opCode) {
        case 0:
          return SUCCESS;
        case 1:
          return FAILURE;
        case 2:
          return GET_RESPONSE;
        default:
          throw new IllegalArgumentException("Store operation not defined for : " + opCode);
      }
    }
  }

  public abstract byte[] encode();

  public abstract Type getType();

  public static EhcacheEntityResponse decode(ByteBuffer response) {
    byte opCode = response.get();
    EhcacheEntityResponse.Type type = EhcacheEntityResponse.Type.responseType(opCode);
    byte[] payArr = new byte[response.remaining()];
    response.get(payArr);
    switch (type) {
      case SUCCESS:
        return Success.INSTANCE;
      case FAILURE:
        return new Failure(ByteBuffer.wrap(payArr));
      case GET_RESPONSE:
        return new GetResponse(ByteBuffer.wrap(payArr));
      default:
        throw new UnsupportedOperationException("The operation is not supported with opCode : " + opCode);
    }
  }

  public static class Success extends EhcacheEntityResponse {

    public static final Success INSTANCE = new Success();

    private Success() {
      //singleton
    }

    @Override
    public Type getType() {
      return Type.SUCCESS;
    }

    @Override
    public byte[] encode() {
      ByteBuffer buffer = ByteBuffer.allocate(OP_CODE_SIZE);
      buffer.put(Type.SUCCESS.getOpCode());
      return buffer.array();
    }
  }

  public static class Failure extends EhcacheEntityResponse {

    private final Exception cause;

    Failure(Exception cause) {
      this.cause = cause;
    }

    Failure(ByteBuffer failure) {
      this.cause = (Exception)Util.unmarshall(failure.array());
    }

    @Override
    public Type getType() {
      return Type.FAILURE;
    }

    public Exception getCause() {
      return cause;
    }

    @Override
    public byte[] encode() {
      byte[] failureMsg = Util.marshall(this.cause);
      ByteBuffer buffer = ByteBuffer.allocate(OP_CODE_SIZE + failureMsg.length);
      buffer.put(Type.FAILURE.getOpCode());
      buffer.put(failureMsg);
      return buffer.array();
    }
  }

  public static class GetResponse extends EhcacheEntityResponse {

    private final Chain chain;

    GetResponse(Chain chain) {
      this.chain = chain;
    }

    GetResponse(ByteBuffer response) {
      this.chain = ChainCodec.decode(response.array());
    }

    @Override
    public Type getType() {
      return Type.GET_RESPONSE;
    }

    public Chain getChain() {
      return chain;
    }

    @Override
    public byte[] encode() {
      byte[] encodedChain = ChainCodec.encode(this.chain);
      int chainLen = encodedChain.length;
      ByteBuffer buffer = ByteBuffer.allocate(OP_CODE_SIZE + chainLen);
      buffer.put(Type.GET_RESPONSE.getOpCode());
      buffer.put(encodedChain);
      return buffer.array();
    }

  }

}
