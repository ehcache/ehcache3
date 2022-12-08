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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.terracotta.client.message.tracker.RecordedMessage;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ClientSourceId;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;

import static org.ehcache.clustered.common.internal.messages.BaseCodec.EHCACHE_MESSAGE_TYPES_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.BaseCodec.MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.common.internal.messages.BaseCodec.MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isPassiveReplicationMessage;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;
/**
 * EhcacheServerCodec
 */
public class EhcacheServerCodec implements MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheServerCodec.class);

  private final EhcacheCodec clientCodec;
  private final PassiveReplicationMessageCodec replicationCodec;

  private static final String CLIENT_ID = "client";
  private static final String TRANSACTION_ID = "transaction";
  private static final String MESSAGE = "message";
  private static final String MESSAGE_SEQUENCE = "sequence";

  private static final Struct RECORDED_MESSAGE = newStructBuilder()
    .int64(CLIENT_ID, 10)
    .int64(TRANSACTION_ID, 20)
    .byteBuffer(MESSAGE, 30)
    .build();

  private static final Struct MESSAGE_HISTORY = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .structs(MESSAGE_SEQUENCE, 20, RECORDED_MESSAGE)
    .build();

  public EhcacheServerCodec(EhcacheCodec clientCodec, PassiveReplicationMessageCodec replicationCodec) {
    this.clientCodec = clientCodec;
    this.replicationCodec = replicationCodec;
  }

  @Override
  public byte[] encodeMessage(EhcacheEntityMessage message) {
    if (message instanceof PassiveReplicationMessage) {
      return replicationCodec.encode((PassiveReplicationMessage) message);
    } else if (message instanceof EhcacheMessageTrackerCatchup) {
      return encodeCatchup((EhcacheMessageTrackerCatchup)message);
    }
    return clientCodec.encodeMessage(message);
  }

  private byte[] encodeCatchup(EhcacheMessageTrackerCatchup catchup) {
    return MESSAGE_HISTORY.encoder()
      .enm(MESSAGE_TYPE_FIELD_NAME, EhcacheMessageType.MESSAGE_CATCHUP)
      .structs(MESSAGE_SEQUENCE, catchup.getTrackedMessages(), (encoder, value) -> {
        encoder.int64(CLIENT_ID, value.getClientSourceId().toLong());
        encoder.int64(TRANSACTION_ID, value.getTransactionId());
        encoder.byteBuffer(MESSAGE, ByteBuffer.wrap(encodeMessage(value.getRequest())));
      }).encode().array();
  }

  @Override
  public EhcacheEntityMessage decodeMessage(byte[] payload) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
    Enm<EhcacheMessageType> opCodeEnm = EhcacheCodec.OP_CODE_DECODER.decoder(byteBuffer).enm(MESSAGE_TYPE_FIELD_NAME);
    if (!opCodeEnm.isFound()) {
      throw new AssertionError("Got a message without an opCode");
    }
    if (!opCodeEnm.isValid()) {
      LOGGER.warn("Received message with unknown operation code - more recent version at the other end?");
      return null;
    }

    byteBuffer.rewind();

    EhcacheMessageType messageType = opCodeEnm.get();
    if (messageType == EhcacheMessageType.MESSAGE_CATCHUP) {
      return decodeCatchup(payload);
    } else if (isPassiveReplicationMessage(messageType)) {
      return replicationCodec.decode(messageType, byteBuffer);
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

  private EhcacheMessageTrackerCatchup decodeCatchup(byte[] payload) {
    StructArrayDecoder<StructDecoder<Void>> array = MESSAGE_HISTORY.decoder(ByteBuffer.wrap(payload)).structs(MESSAGE_SEQUENCE);
    if (array == null) {
      return new EhcacheMessageTrackerCatchup(Collections.emptyList());
    }
    List<RecordedMessage<EhcacheEntityMessage, EhcacheEntityResponse>> list = new ArrayList<>(array.length());
    while (array.hasNext()) {
      StructDecoder<StructArrayDecoder<StructDecoder<Void>>> decoder = array.next();
      long cid = decoder.int64(CLIENT_ID);
      long transaction = decoder.int64(TRANSACTION_ID);
      ByteBuffer buff = decoder.byteBuffer(MESSAGE);
      EhcacheEntityMessage msg = decodeMessage(buff.array());

      list.add(new RecordedMessage<EhcacheEntityMessage, EhcacheEntityResponse>() {
        @Override
        public ClientSourceId getClientSourceId() {
          return new ClientSourceId() {
            @Override
            public long toLong() {
              return cid;
            }

            @Override
            public boolean matches(ClientDescriptor cd) {
              return cd.getSourceId().toLong() == cid;
            }
          };
        }

        @Override
        public long getTransactionId() {
          return transaction;
        }

        @Override
        public EhcacheEntityMessage getRequest() {
          return msg;
        }

        @Override
        public EhcacheEntityResponse getResponse() {
          return null;
        }
      });
    }
    return new EhcacheMessageTrackerCatchup(list);
  }
}
