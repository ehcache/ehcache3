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

import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;

import static org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage.Type.LIFECYCLE_OP;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage.Type.SERVER_STORE_OP;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage.Type.STATE_REPO_OP;

public class EhcacheCodec implements MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> SERVER_INSTANCE =
      new EhcacheCodec(new ServerStoreOpCodec(), new LifeCycleMessageCodec(), new StateRepositoryOpCodec(), new ResponseCodec());

  private final ServerStoreOpCodec serverStoreOpCodec;
  private final LifeCycleMessageCodec lifeCycleMessageCodec;
  private final StateRepositoryOpCodec stateRepositoryOpCodec;
  private final ResponseCodec responseCodec;

  public static MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> messageCodec() {
    return SERVER_INSTANCE;
  }

  EhcacheCodec(ServerStoreOpCodec serverStoreOpCodec, LifeCycleMessageCodec lifeCycleMessageCodec,
               StateRepositoryOpCodec stateRepositoryOpCodec, ResponseCodec responseCodec) {
    this.serverStoreOpCodec = serverStoreOpCodec;
    this.lifeCycleMessageCodec = lifeCycleMessageCodec;
    this.stateRepositoryOpCodec = stateRepositoryOpCodec;
    this.responseCodec = responseCodec;
  }

  @Override
  public byte[] encodeMessage(EhcacheEntityMessage message) {
    switch (message.getType()) {
      case LIFECYCLE_OP:
        return lifeCycleMessageCodec.encode((LifecycleMessage)message);
      case SERVER_STORE_OP:
        return serverStoreOpCodec.encode((ServerStoreOpMessage) message);
      case STATE_REPO_OP:
        return stateRepositoryOpCodec.encode((StateRepositoryOpMessage) message);
      default:
        throw new IllegalArgumentException("Undefined message type: " + message.getType());
    }
  }

  @Override
  public EhcacheEntityMessage decodeMessage(byte[] payload) throws MessageCodecException {
    byte opCode = payload[0];
    if (opCode <= LIFECYCLE_OP.getCode()) {
        return lifeCycleMessageCodec.decode(payload);
    } else if (opCode <= SERVER_STORE_OP.getCode()) {
        return serverStoreOpCodec.decode(payload);
    } else if (opCode <= STATE_REPO_OP.getCode()) {
        return stateRepositoryOpCodec.decode(payload);
    } else {
      throw new UnsupportedOperationException("Undefined message code: " + opCode);
    }
  }

  @Override
  public byte[] encodeResponse(EhcacheEntityResponse response) throws MessageCodecException {
    return responseCodec.encode(response);
  }

  @Override
  public EhcacheEntityResponse decodeResponse(byte[] payload) throws MessageCodecException {
    return responseCodec.decode(payload);
  }
}
