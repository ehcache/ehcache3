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
import org.terracotta.entity.EntityResponse;

/**
 *
 * @author cdennis
 */
public abstract class EhcacheEntityResponse implements EntityResponse {

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

  public abstract Type getType();

  public static class Success extends EhcacheEntityResponse {

    public static final Success INSTANCE = new Success();

    private Success() {
      //singleton
    }

    @Override
    public Type getType() {
      return Type.SUCCESS;
    }

  }

  public static class Failure extends EhcacheEntityResponse {

    private final Exception cause;

    Failure(Exception cause) {
      this.cause = cause;
    }

    @Override
    public Type getType() {
      return Type.FAILURE;
    }

    public Exception getCause() {
      return cause;
    }

  }

  public static class GetResponse extends EhcacheEntityResponse {

    private final Chain chain;

    GetResponse(Chain chain) {
      this.chain = chain;
    }

    @Override
    public Type getType() {
      return Type.GET_RESPONSE;
    }

    public Chain getChain() {
      return chain;
    }

  }

}
