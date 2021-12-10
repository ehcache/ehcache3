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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.Enm;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.wrap;
import static org.ehcache.clustered.common.internal.messages.BaseCodec.EHCACHE_MESSAGE_TYPES_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.BaseCodec.MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.common.internal.messages.BaseCodec.MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isLifecycleMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isStateRepoOperationMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isStoreOperationMessage;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class EhcacheCodec implements MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheCodec.class);

  public static final Struct OP_CODE_DECODER = newStructBuilder().enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING).build();

  private final ServerStoreOpCodec serverStoreOpCodec;
  private final LifeCycleMessageCodec lifeCycleMessageCodec;
  private final StateRepositoryOpCodec stateRepositoryOpCodec;
  private final ResponseCodec responseCodec;

  public EhcacheCodec(ServerStoreOpCodec serverStoreOpCodec, LifeCycleMessageCodec lifeCycleMessageCodec,
               StateRepositoryOpCodec stateRepositoryOpCodec, ResponseCodec responseCodec) {
    this.serverStoreOpCodec = serverStoreOpCodec;
    this.lifeCycleMessageCodec = lifeCycleMessageCodec;
    this.stateRepositoryOpCodec = stateRepositoryOpCodec;
    this.responseCodec = responseCodec;
  }

  @Override
  public byte[] encodeMessage(EhcacheEntityMessage message) {
    if (!(message instanceof EhcacheOperationMessage)) {
      throw new AssertionError("Unexpected message type " + message.getClass());
    }
    EhcacheOperationMessage operationMessage = (EhcacheOperationMessage) message;
    if (isLifecycleMessage(operationMessage.getMessageType())) {
      return lifeCycleMessageCodec.encode((LifecycleMessage) operationMessage);
    } else if (isStoreOperationMessage(operationMessage.getMessageType())) {
      return serverStoreOpCodec.encode((ServerStoreOpMessage) operationMessage);
    } else if (isStateRepoOperationMessage(operationMessage.getMessageType())) {
      return stateRepositoryOpCodec.encode((StateRepositoryOpMessage) operationMessage);
    }
    throw new AssertionError("Unknown message type: " + operationMessage.getMessageType());
  }

  @Override
  public EhcacheEntityMessage decodeMessage(byte[] payload) {
    ByteBuffer byteBuffer = wrap(payload);
    Enm<EhcacheMessageType> opCodeEnm = OP_CODE_DECODER.decoder(byteBuffer).enm("opCode");

    if (!opCodeEnm.isFound()) {
      throw new AssertionError("Got a message without an opCode");
    }
    if (!opCodeEnm.isValid()) {
      LOGGER.warn("Received message with unknown operation code - more recent version at the other end?");
      return null;
    }

    byteBuffer.rewind();

    EhcacheMessageType opCode = opCodeEnm.get();
    return decodeMessage(byteBuffer, opCode);
  }

  public EhcacheEntityMessage decodeMessage(ByteBuffer byteBuffer, EhcacheMessageType opCode) {
    if (isLifecycleMessage(opCode)) {
        return lifeCycleMessageCodec.decode(opCode, byteBuffer);
    } else if (isStoreOperationMessage(opCode)) {
        return serverStoreOpCodec.decode(opCode, byteBuffer);
    } else if (isStateRepoOperationMessage(opCode)) {
        return stateRepositoryOpCodec.decode(opCode, byteBuffer);
    } else {
      throw new UnsupportedOperationException("Unsupported message code: " + opCode);
    }
  }

  @Override
  public byte[] encodeResponse(EhcacheEntityResponse response) {
    return responseCodec.encode(response);
  }

  @Override
  public EhcacheEntityResponse decodeResponse(byte[] payload) {
    return responseCodec.decode(payload);
  }
}
