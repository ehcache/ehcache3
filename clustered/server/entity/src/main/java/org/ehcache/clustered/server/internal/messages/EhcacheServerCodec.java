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

package org.ehcache.clustered.server.internal.messages;

import org.ehcache.clustered.common.internal.messages.EhcacheCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.runnel.decoding.Enm;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.wrap;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isPassiveReplicationMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isReconnectPassiveReplicationMessage;

/**
 * EhcacheServerCodec
 */
public class EhcacheServerCodec implements MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheServerCodec.class);

  private final EhcacheCodec clientCodec;
  private final PassiveReplicationMessageCodec replicationCodec;
  private final ReconnectPassiveReplicationMessageCodec reconnectPassiveReplicationMessageCodec;

  public EhcacheServerCodec(EhcacheCodec clientCodec, PassiveReplicationMessageCodec replicationCodec, ReconnectPassiveReplicationMessageCodec reconnectPassiveReplicationMessageCodec) {
    this.clientCodec = clientCodec;
    this.replicationCodec = replicationCodec;
    this.reconnectPassiveReplicationMessageCodec = reconnectPassiveReplicationMessageCodec;
  }

  @Override
  public byte[] encodeMessage(EhcacheEntityMessage message) {
    if (message instanceof PassiveReplicationMessage) {
      return replicationCodec.encode((PassiveReplicationMessage) message);
    }

    if (message instanceof ReconnectPassiveReplicationMessage) {
      return this.reconnectPassiveReplicationMessageCodec.encode((ReconnectPassiveReplicationMessage)message, this);
    }

    return clientCodec.encodeMessage(message);
  }

  @Override
  public EhcacheEntityMessage decodeMessage(byte[] payload) {
    ByteBuffer byteBuffer = wrap(payload);
    Enm<EhcacheMessageType> opCodeEnm = EhcacheCodec.OP_CODE_DECODER.decoder(byteBuffer).enm("opCode");
    if (!opCodeEnm.isFound()) {
      throw new AssertionError("Got a message without an opCode");
    }
    if (!opCodeEnm.isValid()) {
      LOGGER.warn("Received message with unknown operation code - more recent version at the other end?");
      return null;
    }

    byteBuffer.rewind();

    EhcacheMessageType messageType = opCodeEnm.get();
    if (isPassiveReplicationMessage(messageType)) {
      return replicationCodec.decode(messageType, byteBuffer);
    }

    if (isReconnectPassiveReplicationMessage(messageType)) {
      return reconnectPassiveReplicationMessageCodec.decode(byteBuffer, this);
    }

    return clientCodec.decodeMessage(byteBuffer, messageType);
  }

  @Override
  public byte[] encodeResponse(EhcacheEntityResponse response) throws MessageCodecException {
    return clientCodec.encodeResponse(response);
  }

  @Override
  public EhcacheEntityResponse decodeResponse(byte[] payload) throws MessageCodecException {
    return clientCodec.decodeResponse(payload);
  }
}
