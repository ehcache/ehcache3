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

import org.ehcache.clustered.common.internal.store.Util;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.UUID;

import static java.nio.ByteBuffer.wrap;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.EHCACHE_MESSAGE_TYPES_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.KEY_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.LSB_UUID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.MSB_UUID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.MSG_ID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.SERVER_STORE_NAME_FIELD;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class StateRepositoryOpCodec {

  private static final String MAP_ID_FIELD = "mapId";
  private static final String VALUE_FIELD = "value";

  private static final Struct GET_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .string(MAP_ID_FIELD, 35)
    .byteBuffer(KEY_FIELD, 40)
    .build();

  private static final Struct PUT_IF_ABSENT_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .string(MAP_ID_FIELD, 35)
    .byteBuffer(KEY_FIELD, 40)
    .byteBuffer(VALUE_FIELD, 45)
    .build();

  private static final Struct ENTRY_SET_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .string(MAP_ID_FIELD, 35)
    .build();

  private final MessageCodecUtils messageCodecUtils = new MessageCodecUtils();

  public byte[] encode(StateRepositoryOpMessage message) {

    switch (message.getMessageType()) {
      case GET_STATE_REPO:
        return encodeGetMessage((StateRepositoryOpMessage.GetMessage) message);
      case PUT_IF_ABSENT:
        return encodePutIfAbsentMessage((StateRepositoryOpMessage.PutIfAbsentMessage) message);
      case ENTRY_SET:
        return encodeEntrySetMessage((StateRepositoryOpMessage.EntrySetMessage) message);
      default:
        throw new IllegalArgumentException("Unsupported StateRepositoryOpMessage " + message.getClass());
    }
  }

  private byte[] encodeEntrySetMessage(StateRepositoryOpMessage.EntrySetMessage message) {
    StructEncoder<Void> encoder = ENTRY_SET_MESSAGE_STRUCT.encoder();

    messageCodecUtils.encodeMandatoryFields(encoder, message);
    encoder.string(SERVER_STORE_NAME_FIELD, message.getCacheId());
    encoder.string(MAP_ID_FIELD, message.getCacheId());

    return encoder.encode().array();
  }

  private byte[] encodePutIfAbsentMessage(StateRepositoryOpMessage.PutIfAbsentMessage message) {
    StructEncoder<Void> encoder = PUT_IF_ABSENT_MESSAGE_STRUCT.encoder();

    messageCodecUtils.encodeMandatoryFields(encoder, message);
    encoder.string(SERVER_STORE_NAME_FIELD, message.getCacheId());
    encoder.string(MAP_ID_FIELD, message.getCacheId());
    // TODO this needs to change - serialization needs to happen in the StateRepo not here, though we need the hashcode for server side comparison.
    encoder.byteBuffer(KEY_FIELD, wrap(Util.marshall(message.getKey())));
    encoder.byteBuffer(VALUE_FIELD, wrap(Util.marshall(message.getValue())));

    return encoder.encode().array();
  }

  private byte[] encodeGetMessage(StateRepositoryOpMessage.GetMessage message) {
    StructEncoder<Void> encoder = GET_MESSAGE_STRUCT.encoder();

    messageCodecUtils.encodeMandatoryFields(encoder, message);
    encoder.string(SERVER_STORE_NAME_FIELD, message.getCacheId());
    encoder.string(MAP_ID_FIELD, message.getCacheId());
    // TODO this needs to change - serialization needs to happen in the StateRepo not here, though we need the hashcode for server side comparison.
    encoder.byteBuffer(KEY_FIELD, wrap(Util.marshall(message.getKey())));

    return encoder.encode().array();
  }

  public StateRepositoryOpMessage decode(EhcacheMessageType messageType, ByteBuffer messageBuffer) {
    switch (messageType) {
      case GET_STATE_REPO:
        return decodeGetMessage(messageBuffer);
      case PUT_IF_ABSENT:
        return decodePutIfAbsentMessage(messageBuffer);
      case ENTRY_SET:
        return decodeEntrySetMessage(messageBuffer);
      default:
        throw new IllegalArgumentException("Unsupported StateRepositoryOpMessage " + messageType);
    }
  }

  private StateRepositoryOpMessage.EntrySetMessage decodeEntrySetMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = ENTRY_SET_MESSAGE_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID clientId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);
    String mapId = decoder.string(MAP_ID_FIELD);

    StateRepositoryOpMessage.EntrySetMessage message = new StateRepositoryOpMessage.EntrySetMessage(storeName, mapId, clientId);
    message.setId(msgId);
    return message;
  }

  private StateRepositoryOpMessage.PutIfAbsentMessage decodePutIfAbsentMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = PUT_IF_ABSENT_MESSAGE_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID clientId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);
    String mapId = decoder.string(MAP_ID_FIELD);

    ByteBuffer keyBuffer = decoder.byteBuffer(KEY_FIELD);
    Object key = Util.unmarshall(keyBuffer);

    ByteBuffer valueBuffer = decoder.byteBuffer(VALUE_FIELD);
    Object value = Util.unmarshall(valueBuffer);

    StateRepositoryOpMessage.PutIfAbsentMessage message = new StateRepositoryOpMessage.PutIfAbsentMessage(storeName, mapId, key, value, clientId);
    message.setId(msgId);
    return message;
  }

  private StateRepositoryOpMessage.GetMessage decodeGetMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = GET_MESSAGE_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID clientId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);
    String mapId = decoder.string(MAP_ID_FIELD);

    ByteBuffer keyBuffer = decoder.byteBuffer(KEY_FIELD);
    Object key = Util.unmarshall(keyBuffer);

    StateRepositoryOpMessage.GetMessage getMessage = new StateRepositoryOpMessage.GetMessage(storeName, mapId, key, clientId);
    getMessage.setId(msgId);
    return getMessage;
  }
}
