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

import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.AppendMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ClearMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ClientInvalidationAck;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ClientInvalidationAllAck;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.GetAndAppendMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.GetMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ReplaceAtHeadMessage;
import org.ehcache.clustered.common.internal.store.Chain;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.ehcache.clustered.common.internal.messages.ChainCodec.CHAIN_ENCODER_FUNCTION;
import static org.ehcache.clustered.common.internal.messages.ChainCodec.CHAIN_STRUCT;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.EHCACHE_MESSAGE_TYPES_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.KEY_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.LSB_UUID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.MSB_UUID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.MSG_ID_FIELD;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class ServerStoreOpCodec {

  private static final Struct GET_AND_APPEND_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .int64(KEY_FIELD, 30)
    .byteBuffer("payload", 40)
    .build();

  private static final Struct APPEND_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .int64(KEY_FIELD, 30)
    .byteBuffer("payload", 40)
    .build();

  private static final Struct REPLACE_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .int64(KEY_FIELD, 30)
    .struct("expect", 40, CHAIN_STRUCT)
    .struct("update", 50, CHAIN_STRUCT)
    .build();

  private static final Struct CLIENT_INVALIDATION_ACK_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(KEY_FIELD, 20)
    .int32("invalidationId", 30)
    .build();

  private static final Struct CLIENT_INVALIDATION_ALL_ACK_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int32("invalidationId", 40)
    .build();

  private static final Struct CLEAR_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .build();

  private static final Struct GET_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(KEY_FIELD, 30)
    .build();

  private final MessageCodecUtils messageCodecUtils = new MessageCodecUtils();

  public byte[] encode(ServerStoreOpMessage message) {
    StructEncoder<Void> encoder;

    switch (message.getMessageType()) {
      case GET_STORE:
        GetMessage getMessage = (GetMessage) message;
        encoder = GET_MESSAGE_STRUCT.encoder();
        return encoder
          .enm(MESSAGE_TYPE_FIELD_NAME, message.getMessageType())
          .int64(MSG_ID_FIELD, message.getId())
          .int64(KEY_FIELD, getMessage.getKey())
          .encode()
          .array();
      case APPEND:
        AppendMessage appendMessage = (AppendMessage) message;
        encoder = APPEND_MESSAGE_STRUCT.encoder();
        messageCodecUtils.encodeMandatoryFields(encoder, message);
        return encoder
          .int64(KEY_FIELD, appendMessage.getKey())
          .byteBuffer("payload", appendMessage.getPayload())
          .encode()
          .array();
      case GET_AND_APPEND:
        GetAndAppendMessage getAndAppendMessage = (GetAndAppendMessage) message;
        encoder = GET_AND_APPEND_MESSAGE_STRUCT.encoder();
        messageCodecUtils.encodeMandatoryFields(encoder, message);
        return encoder
          .int64(KEY_FIELD, getAndAppendMessage.getKey())
          .byteBuffer("payload", getAndAppendMessage.getPayload())
          .encode()
          .array();
      case REPLACE:
        final ReplaceAtHeadMessage replaceAtHeadMessage = (ReplaceAtHeadMessage) message;
        encoder = REPLACE_MESSAGE_STRUCT.encoder();
        messageCodecUtils.encodeMandatoryFields(encoder, message);
        return encoder
          .int64(KEY_FIELD, replaceAtHeadMessage.getKey())
          .struct("expect", replaceAtHeadMessage.getExpect(), CHAIN_ENCODER_FUNCTION)
          .struct("update", replaceAtHeadMessage.getUpdate(), CHAIN_ENCODER_FUNCTION)
          .encode()
          .array();
      case CLIENT_INVALIDATION_ACK:
        ClientInvalidationAck clientInvalidationAckMessage = (ClientInvalidationAck) message;
        encoder = CLIENT_INVALIDATION_ACK_MESSAGE_STRUCT.encoder();
        return encoder
          .enm(MESSAGE_TYPE_FIELD_NAME, message.getMessageType())
          .int64(MSG_ID_FIELD, message.getId())
          .int64(KEY_FIELD, clientInvalidationAckMessage.getKey())
          .int32("invalidationId", clientInvalidationAckMessage.getInvalidationId())
          .encode()
          .array();
      case CLIENT_INVALIDATION_ALL_ACK:
        ClientInvalidationAllAck clientInvalidationAllAckMessage = (ClientInvalidationAllAck) message;
        encoder = CLIENT_INVALIDATION_ALL_ACK_MESSAGE_STRUCT.encoder();
        return encoder
          .enm(MESSAGE_TYPE_FIELD_NAME, message.getMessageType())
          .int64(MSG_ID_FIELD, message.getId())
          .int32("invalidationId", clientInvalidationAllAckMessage.getInvalidationId())
          .encode().array();
      case CLEAR:
        encoder = CLEAR_MESSAGE_STRUCT.encoder();
        messageCodecUtils.encodeMandatoryFields(encoder, message);
        return encoder
          .encode()
          .array();
      default:
        throw new RuntimeException("Unhandled message operation : " + message.getMessageType());
    }
  }

  public EhcacheEntityMessage decode(EhcacheMessageType opCode, ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder;
    switch (opCode) {
      case GET_STORE: {
        decoder = GET_MESSAGE_STRUCT.decoder(messageBuffer);
        Long msgId = decoder.int64(MSG_ID_FIELD);
        Long key = decoder.int64(KEY_FIELD);
        GetMessage message = new GetMessage(key);
        message.setId(msgId);
        return message;
      }
      case GET_AND_APPEND: {
        decoder = GET_AND_APPEND_MESSAGE_STRUCT.decoder(messageBuffer);
        Long msgId = decoder.int64(MSG_ID_FIELD);
        UUID uuid = messageCodecUtils.decodeUUID(decoder);
        Long key = decoder.int64(KEY_FIELD);
        ByteBuffer payload = decoder.byteBuffer("payload");
        GetAndAppendMessage message = new GetAndAppendMessage(key, payload, uuid);
        message.setId(msgId);
        return message;
      }
      case APPEND: {
        decoder = APPEND_MESSAGE_STRUCT.decoder(messageBuffer);
        Long msgId = decoder.int64(MSG_ID_FIELD);
        UUID uuid = messageCodecUtils.decodeUUID(decoder);
        Long key = decoder.int64(KEY_FIELD);
        ByteBuffer payload = decoder.byteBuffer("payload");
        AppendMessage message = new AppendMessage(key, payload, uuid);
        message.setId(msgId);
        return message;
      }
      case REPLACE: {
        decoder = REPLACE_MESSAGE_STRUCT.decoder(messageBuffer);
        Long msgId = decoder.int64(MSG_ID_FIELD);
        UUID uuid = messageCodecUtils.decodeUUID(decoder);
        Long key = decoder.int64(KEY_FIELD);
        Chain expect = ChainCodec.decode(decoder.struct("expect"));
        Chain update = ChainCodec.decode(decoder.struct("update"));
        ReplaceAtHeadMessage message = new ReplaceAtHeadMessage(key, expect, update, uuid);
        message.setId(msgId);
        return message;
      }
      case CLIENT_INVALIDATION_ACK: {
        decoder = CLIENT_INVALIDATION_ACK_MESSAGE_STRUCT.decoder(messageBuffer);
        Long msgId = decoder.int64(MSG_ID_FIELD);
        Long key = decoder.int64(KEY_FIELD);
        Integer invalidationId = decoder.int32("invalidationId");
        ClientInvalidationAck message = new ClientInvalidationAck(key, invalidationId);
        message.setId(msgId);
        return message;
      }
      case CLIENT_INVALIDATION_ALL_ACK: {
        decoder = CLIENT_INVALIDATION_ALL_ACK_MESSAGE_STRUCT
          .decoder(messageBuffer);
        Long msgId = decoder.int64(MSG_ID_FIELD);
        Integer invalidationId = decoder.int32("invalidationId");
        ClientInvalidationAllAck message = new ClientInvalidationAllAck(invalidationId);
        message.setId(msgId);
        return message;
      }
      case CLEAR: {
        decoder = CLEAR_MESSAGE_STRUCT.decoder(messageBuffer);
        Long msgId = decoder.int64(MSG_ID_FIELD);
        UUID uuid = messageCodecUtils.decodeUUID(decoder);
        ClearMessage message = new ClearMessage(uuid);
        message.setId(msgId);
        return message;
      }
      default:
        throw new RuntimeException("Unhandled message operation : " + opCode);
    }
  }

}
