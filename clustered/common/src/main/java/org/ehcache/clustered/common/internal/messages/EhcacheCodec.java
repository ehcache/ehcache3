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

public class EhcacheCodec implements MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> SERVER_INSTANCE =
      new EhcacheCodec(new ServerStoreOpCodec(), new LifeCycleMessageCodec(), new ResponseCodec());

  private final ServerStoreOpCodec serverStoreOpCodec;
  private final LifeCycleMessageCodec lifeCycleMessageCodec;
  private final ResponseCodec responseCodec;

  public static MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> messageCodec() {
    return SERVER_INSTANCE;
  }

  private EhcacheCodec(ServerStoreOpCodec serverStoreOpCodec, LifeCycleMessageCodec lifeCycleMessageCodec, ResponseCodec responseCodec) {
    this.serverStoreOpCodec = serverStoreOpCodec;
    this.lifeCycleMessageCodec = lifeCycleMessageCodec;
    this.responseCodec = responseCodec;
  }

  @Override
  public byte[] encodeMessage(EhcacheEntityMessage message) {
    if (message.getType() == EhcacheEntityMessage.Type.LIFECYCLE_OP) {
      return lifeCycleMessageCodec.encode((LifecycleMessage) message);
    } else {
      return serverStoreOpCodec.encode((ServerStoreOpMessage) message);
    }
  }

  @Override
  public EhcacheEntityMessage decodeMessage(byte[] payload) throws MessageCodecException {
    byte opcode = payload[0];
    if (opcode == LifecycleMessage.LIFECYCLE_MSG_OP_CODE) {
      return lifeCycleMessageCodec.decode(payload);
    } else {
      return serverStoreOpCodec.decode(payload);
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
