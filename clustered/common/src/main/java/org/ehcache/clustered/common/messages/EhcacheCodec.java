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

import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;

/**
 *
 * @author cdennis
 */
public class EhcacheCodec implements MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> SERVER_INSTANCE = new EhcacheCodec();

  public static MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> messageCodec() {
    return SERVER_INSTANCE;
  }

  @Override
  public byte[] encodeMessage(EhcacheEntityMessage message) {
    return message.encode();
  }

  @Override
  public EhcacheEntityMessage decodeMessage(byte[] payload) throws MessageCodecException {
    ByteBuffer payloadBuf = ByteBuffer.wrap(payload);
    byte opcode = payloadBuf.get();
    payloadBuf.rewind();
    if (opcode == EhcacheEntityMessage.Type.LIFECYCLE_OP.getOpCode()) {
      return LifecycleMessage.decode(payloadBuf);
    } else {
      return ServerStoreOpMessage.decode(payloadBuf);
    }
  }

  @Override
  public byte[] encodeResponse(EhcacheEntityResponse response) throws MessageCodecException {
    return response.encode();
  }

  @Override
  public EhcacheEntityResponse decodeResponse(byte[] payload) throws MessageCodecException {
    return EhcacheEntityResponse.decode(ByteBuffer.wrap(payload));
  }
}
