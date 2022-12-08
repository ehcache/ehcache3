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

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.EHCACHE_MESSAGE_TYPES_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.LSB_UUID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.MSB_UUID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.MSG_ID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.SERVER_STORE_NAME_FIELD;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class LifeCycleMessageCodec {

  private static final String CONFIG_PRESENT_FIELD = "configPresent";

  private final StructBuilder VALIDATE_MESSAGE_STRUCT_BUILDER_PREFIX = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .bool(CONFIG_PRESENT_FIELD, 30);
  private static final int CONFIGURE_MESSAGE_NEXT_INDEX = 40;

  private final StructBuilder VALIDATE_STORE_MESSAGE_STRUCT_BUILDER_PREFIX = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30);
  private static final int VALIDATE_STORE_NEXT_INDEX = 40;

  private final Struct PREPARE_FOR_DESTROY_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .build();

  private final Struct validateMessageStruct;
  private final Struct validateStoreMessageStruct;

  private final MessageCodecUtils messageCodecUtils;
  private final ConfigCodec configCodec;

  public LifeCycleMessageCodec(ConfigCodec configCodec) {
    this.messageCodecUtils = new MessageCodecUtils();
    this.configCodec = configCodec;
    validateMessageStruct = this.configCodec.injectServerSideConfiguration(
      VALIDATE_MESSAGE_STRUCT_BUILDER_PREFIX, CONFIGURE_MESSAGE_NEXT_INDEX).getUpdatedBuilder().build();

    validateStoreMessageStruct = this.configCodec.injectServerStoreConfiguration(
      VALIDATE_STORE_MESSAGE_STRUCT_BUILDER_PREFIX, VALIDATE_STORE_NEXT_INDEX).getUpdatedBuilder().build();
  }

  public byte[] encode(LifecycleMessage message) {
    switch (message.getMessageType()) {
      case VALIDATE:
        return encodeTierManagerValidateMessage((LifecycleMessage.ValidateStoreManager) message);
      case VALIDATE_SERVER_STORE:
        return encodeValidateStoreMessage((LifecycleMessage.ValidateServerStore) message);
      case PREPARE_FOR_DESTROY:
        return encodePrepareForDestroyMessage(message);
      default:
        throw new IllegalArgumentException("Unknown lifecycle message: " + message.getClass());
    }
  }

  private byte[] encodePrepareForDestroyMessage(LifecycleMessage message) {
    return PREPARE_FOR_DESTROY_STRUCT.encoder()
      .enm(MESSAGE_TYPE_FIELD_NAME, message.getMessageType())
      .encode().array();
  }

  private byte[] encodeValidateStoreMessage(LifecycleMessage.ValidateServerStore message) {
    StructEncoder<Void> encoder = validateStoreMessageStruct.encoder();
    messageCodecUtils.encodeMandatoryFields(encoder, message);

    encoder.string(SERVER_STORE_NAME_FIELD, message.getName());
    configCodec.encodeServerStoreConfiguration(encoder, message.getStoreConfiguration());
    return encoder.encode().array();
  }

  private byte[] encodeTierManagerValidateMessage(LifecycleMessage.ValidateStoreManager message) {
    StructEncoder<Void> encoder = validateMessageStruct.encoder();
    ServerSideConfiguration config = message.getConfiguration();
    messageCodecUtils.encodeMandatoryFields(encoder, message);
    if (config == null) {
      encoder.bool(CONFIG_PRESENT_FIELD, false);
    } else {
      encoder.bool(CONFIG_PRESENT_FIELD, true);
      configCodec.encodeServerSideConfiguration(encoder, config);
    }
    return encoder.encode().array();
  }

  public EhcacheEntityMessage decode(EhcacheMessageType messageType, ByteBuffer messageBuffer) {

    switch (messageType) {
      case VALIDATE:
        return decodeValidateMessage(messageBuffer);
      case VALIDATE_SERVER_STORE:
        return decodeValidateServerStoreMessage(messageBuffer);
      case PREPARE_FOR_DESTROY:
        return decodePrepareForDestroyMessage();
    }
    throw new IllegalArgumentException("LifeCycleMessage operation not defined for : " + messageType);
  }

  private LifecycleMessage.PrepareForDestroy decodePrepareForDestroyMessage() {
    return new LifecycleMessage.PrepareForDestroy();
  }

  private LifecycleMessage.ValidateServerStore decodeValidateServerStoreMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = validateStoreMessageStruct.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID cliendId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);
    ServerStoreConfiguration config = configCodec.decodeServerStoreConfiguration(decoder);

    LifecycleMessage.ValidateServerStore message = new LifecycleMessage.ValidateServerStore(storeName, config, cliendId);
    message.setId(msgId);
    return message;
  }

  private LifecycleMessage.ValidateStoreManager decodeValidateMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = validateMessageStruct.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID cliendId = messageCodecUtils.decodeUUID(decoder);
    boolean configPresent = decoder.bool(CONFIG_PRESENT_FIELD);

    ServerSideConfiguration config = null;
    if (configPresent) {
      config = configCodec.decodeServerSideConfiguration(decoder);
    }


    LifecycleMessage.ValidateStoreManager message = new LifecycleMessage.ValidateStoreManager(config, cliendId);
    if (msgId != null) {
      message.setId(msgId);
    }
    return message;
  }
}
