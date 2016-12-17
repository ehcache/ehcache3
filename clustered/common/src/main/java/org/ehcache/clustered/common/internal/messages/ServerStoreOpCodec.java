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
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.GetAndAppendMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.GetMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ReplaceAtHeadMessage;
import org.ehcache.clustered.common.internal.store.Chain;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;
import org.terracotta.runnel.encoding.StructEncoderFunction;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.ehcache.clustered.common.internal.messages.ChainCodec.CHAIN_STRUCT;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.EHCACHE_MESSAGE_TYPES_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.KEY_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.LSB_UUID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.MSB_UUID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.MSG_ID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.SERVER_STORE_NAME_FIELD;

public class ServerStoreOpCodec {

  private static final Struct GET_AND_APPEND_MESSAGE_STRUCT = StructBuilder.newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .int64(KEY_FIELD, 40)
    .byteBuffer("payload", 50)
    .build();

  private static final Struct APPEND_MESSAGE_STRUCT = StructBuilder.newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .int64(KEY_FIELD, 40)
    .byteBuffer("payload", 50)
    .build();

  private static final Struct REPLACE_MESSAGE_STRUCT = StructBuilder.newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .int64(KEY_FIELD, 40)
    .struct("expect", 50, CHAIN_STRUCT)
    .struct("update", 60, CHAIN_STRUCT)
    .build();

  private static final Struct CLIENT_INVALIDATION_ACK_MESSAGE_STRUCT = StructBuilder.newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .int32("invalidationId", 40)
    .build();

  private static final Struct CLEAR_MESSAGE_STRUCT = StructBuilder.newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .build();

  private static final Struct GET_MESSAGE_STRUCT = StructBuilder.newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .int64(KEY_FIELD, 40)
    .build();

  private final ChainCodec chainCodec;
  private final MessageCodecUtils messageCodecUtils = new MessageCodecUtils();

  public ServerStoreOpCodec() {
    this.chainCodec = new ChainCodec();
  }

  public byte[] encode(ServerStoreOpMessage message) {
    StructEncoder<Void> encoder = null;

    switch (message.getMessageType()) {
      case GET_STORE:
        GetMessage getMessage = (GetMessage) message;
        encoder = GET_MESSAGE_STRUCT.encoder();
        return encoder
          .enm(MESSAGE_TYPE_FIELD_NAME, message.getMessageType())
          .int64(MSG_ID_FIELD, message.getId())
          .string(SERVER_STORE_NAME_FIELD, getMessage.getCacheId())
          .int64(KEY_FIELD, getMessage.getKey())
          .encode()
          .array();
      case APPEND:
        AppendMessage appendMessage = (AppendMessage) message;
        encoder = APPEND_MESSAGE_STRUCT.encoder();
        messageCodecUtils.encodeMandatoryFields(encoder, message);
        return encoder
          .string(SERVER_STORE_NAME_FIELD, appendMessage.getCacheId())
          .int64(KEY_FIELD, appendMessage.getKey())
          .byteBuffer("payload", appendMessage.getPayload())
          .encode()
          .array();
      case GET_AND_APPEND:
        GetAndAppendMessage getAndAppendMessage = (GetAndAppendMessage) message;
        encoder = GET_AND_APPEND_MESSAGE_STRUCT.encoder();
        messageCodecUtils.encodeMandatoryFields(encoder, message);
        return encoder
          .string(SERVER_STORE_NAME_FIELD, getAndAppendMessage.getCacheId())
          .int64(KEY_FIELD, getAndAppendMessage.getKey())
          .byteBuffer("payload", getAndAppendMessage.getPayload())
          .encode()
          .array();
      case REPLACE:
        final ReplaceAtHeadMessage replaceAtHeadMessage = (ReplaceAtHeadMessage) message;
        encoder = REPLACE_MESSAGE_STRUCT.encoder();
        messageCodecUtils.encodeMandatoryFields(encoder, message);
        return encoder
          .string(SERVER_STORE_NAME_FIELD, replaceAtHeadMessage.getCacheId())
          .int64(KEY_FIELD, replaceAtHeadMessage.getKey())
          .struct("expect", new StructEncoderFunction<StructEncoder<StructEncoder<Void>>>() {
            @Override
            public void encode(StructEncoder<StructEncoder<Void>> encoder) {
              Chain expect = replaceAtHeadMessage.getExpect();
              chainCodec.encode(encoder, expect);
            }
          })
          .struct("update", new StructEncoderFunction<StructEncoder<StructEncoder<Void>>>() {
            @Override
            public void encode(StructEncoder<StructEncoder<Void>> encoder) {
              Chain update = replaceAtHeadMessage.getUpdate();
              chainCodec.encode(encoder, update);
            }
          })
          .encode()
          .array();
      case CLIENT_INVALIDATION_ACK:
        ClientInvalidationAck clientInvalidationAckMessage = (ClientInvalidationAck) message;
        encoder = CLIENT_INVALIDATION_ACK_MESSAGE_STRUCT.encoder();
        return encoder
          .enm(MESSAGE_TYPE_FIELD_NAME, message.getMessageType())
          .int64(MSG_ID_FIELD, message.getId())
          .string(SERVER_STORE_NAME_FIELD, clientInvalidationAckMessage.getCacheId())
          .int32("invalidationId", clientInvalidationAckMessage.getInvalidationId())
          .encode()
          .array();
      case CLEAR:
        ClearMessage clearMessage = (ClearMessage) message;
        encoder = CLEAR_MESSAGE_STRUCT.encoder();
        messageCodecUtils.encodeMandatoryFields(encoder, message);
        return encoder
          .string(SERVER_STORE_NAME_FIELD, clearMessage.getCacheId())
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
        String cacheId = decoder.string(SERVER_STORE_NAME_FIELD);
        Long key = decoder.int64(KEY_FIELD);
        GetMessage message = new GetMessage(cacheId, key);
        message.setId(msgId);
        return message;
      }
      case GET_AND_APPEND: {
        decoder = GET_AND_APPEND_MESSAGE_STRUCT.decoder(messageBuffer);
        Long msgId = decoder.int64(MSG_ID_FIELD);
        UUID uuid = messageCodecUtils.decodeUUID(decoder);
        String cacheId = decoder.string(SERVER_STORE_NAME_FIELD);
        Long key = decoder.int64(KEY_FIELD);
        ByteBuffer payload = decoder.byteBuffer("payload");
        GetAndAppendMessage message = new GetAndAppendMessage(cacheId, key, payload, uuid);
        message.setId(msgId);
        return message;
      }
      case APPEND: {
        decoder = APPEND_MESSAGE_STRUCT.decoder(messageBuffer);
        Long msgId = decoder.int64(MSG_ID_FIELD);
        UUID uuid = messageCodecUtils.decodeUUID(decoder);
        String cacheId = decoder.string(SERVER_STORE_NAME_FIELD);
        Long key = decoder.int64(KEY_FIELD);
        ByteBuffer payload = decoder.byteBuffer("payload");
        AppendMessage message = new AppendMessage(cacheId, key, payload, uuid);
        message.setId(msgId);
        return message;
      }
      case REPLACE: {
        decoder = REPLACE_MESSAGE_STRUCT.decoder(messageBuffer);
        Long msgId = decoder.int64(MSG_ID_FIELD);
        UUID uuid = messageCodecUtils.decodeUUID(decoder);
        String cacheId = decoder.string(SERVER_STORE_NAME_FIELD);
        Long key = decoder.int64(KEY_FIELD);
        Chain expect = chainCodec.decode(decoder.struct("expect"));
        Chain update = chainCodec.decode(decoder.struct("update"));
        ReplaceAtHeadMessage message = new ReplaceAtHeadMessage(cacheId, key, expect, update, uuid);
        message.setId(msgId);
        return message;
      }
      case CLIENT_INVALIDATION_ACK: {
        decoder = CLIENT_INVALIDATION_ACK_MESSAGE_STRUCT.decoder(messageBuffer);
        Long msgId = decoder.int64(MSG_ID_FIELD);
        String cacheId = decoder.string(SERVER_STORE_NAME_FIELD);
        Integer invalidationId = decoder.int32("invalidationId");
        ClientInvalidationAck message = new ClientInvalidationAck(cacheId, invalidationId);
        message.setId(msgId);
        return message;
      }
      case CLEAR: {
        decoder = CLEAR_MESSAGE_STRUCT.decoder(messageBuffer);
        Long msgId = decoder.int64(MSG_ID_FIELD);
        UUID uuid = messageCodecUtils.decodeUUID(decoder);
        String cacheId = decoder.string(SERVER_STORE_NAME_FIELD);
        ClearMessage message = new ClearMessage(cacheId, uuid);
        message.setId(msgId);
        return message;
      }
      default:
        throw new RuntimeException("Unhandled message operation : " + opCode);
    }
  }

}
