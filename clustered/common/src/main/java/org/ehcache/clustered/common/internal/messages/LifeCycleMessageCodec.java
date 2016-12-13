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

  private final StructBuilder CONFIGURE_MESSAGE_STRUCT_BUILDER_PREFIX = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .bool(CONFIG_PRESENT_FIELD, 30);
  private static final int CONFIGURE_MESSAGE_NEXT_INDEX = 40;

  private final StructBuilder CREATE_STORE_MESSAGE_STRUCT_BUILDER_PREFIX = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30);
  private static final int CREATE_STORE_NEXT_INDEX = 40;

  private static final Struct DESTROY_STORE_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .build();

  private final Struct RELEASE_STORE_MESSAGE_STRUCT = DESTROY_STORE_MESSAGE_STRUCT;

  private final Struct configureMessageStruct;
  private final Struct validateMessageStruct;
  private final Struct createStoreMessageStruct;
  private final Struct validateStoreMessageStruct;

  private final MessageCodecUtils messageCodecUtils;
  private final ConfigCodec configCodec;

  public LifeCycleMessageCodec(ConfigCodec configCodec) {
    this.messageCodecUtils = new MessageCodecUtils();
    this.configCodec = configCodec;
    configureMessageStruct = this.configCodec.injectServerSideConfiguration(
      CONFIGURE_MESSAGE_STRUCT_BUILDER_PREFIX, CONFIGURE_MESSAGE_NEXT_INDEX).getUpdatedBuilder().build();
    validateMessageStruct = configureMessageStruct;

    createStoreMessageStruct = this.configCodec.injectServerStoreConfiguration(
      CREATE_STORE_MESSAGE_STRUCT_BUILDER_PREFIX, CREATE_STORE_NEXT_INDEX).getUpdatedBuilder().build();
    validateStoreMessageStruct = createStoreMessageStruct;
  }

  public byte[] encode(LifecycleMessage message) {
    //For configure message id serves as message creation timestamp
    if (message instanceof LifecycleMessage.ConfigureStoreManager) {
      message.setId(System.nanoTime());
    }

    switch (message.getMessageType()) {
      case CONFIGURE:
        return encodeTierManagerConfigureMessage((LifecycleMessage.ConfigureStoreManager) message);
      case VALIDATE:
        return encodeTierManagerValidateMessage((LifecycleMessage.ValidateStoreManager) message);
      case CREATE_SERVER_STORE:
        return encodeCreateStoreMessage((LifecycleMessage.CreateServerStore) message);
      case VALIDATE_SERVER_STORE:
        return encodeValidateStoreMessage((LifecycleMessage.ValidateServerStore) message);
      case DESTROY_SERVER_STORE:
        return encodeDestroyStoreMessage((LifecycleMessage.DestroyServerStore) message);
      case RELEASE_SERVER_STORE:
        return encodeReleaseStoreMessage((LifecycleMessage.ReleaseServerStore) message);
      default:
        throw new IllegalArgumentException("Unknown lifecycle message: " + message.getClass());
    }
  }

  private byte[] encodeReleaseStoreMessage(LifecycleMessage.ReleaseServerStore message) {
    StructEncoder<Void> encoder = RELEASE_STORE_MESSAGE_STRUCT.encoder();

    messageCodecUtils.encodeMandatoryFields(encoder, message);
    encoder.string(SERVER_STORE_NAME_FIELD, message.getName());
    return encoder.encode().array();
  }

  private byte[] encodeDestroyStoreMessage(LifecycleMessage.DestroyServerStore message) {
    StructEncoder<Void> encoder = DESTROY_STORE_MESSAGE_STRUCT.encoder();

    messageCodecUtils.encodeMandatoryFields(encoder, message);
    encoder.string(SERVER_STORE_NAME_FIELD, message.getName());
    return encoder.encode().array();
  }

  private byte[] encodeCreateStoreMessage(LifecycleMessage.CreateServerStore message) {
    StructEncoder<Void> encoder = createStoreMessageStruct.encoder();
    return encodeBaseServerStoreMessage(message, encoder);
  }

  private byte[] encodeValidateStoreMessage(LifecycleMessage.ValidateServerStore message) {
    return encodeBaseServerStoreMessage(message, validateStoreMessageStruct.encoder());
  }

  private byte[] encodeBaseServerStoreMessage(LifecycleMessage.BaseServerStore message, StructEncoder<Void> encoder) {
    messageCodecUtils.encodeMandatoryFields(encoder, message);

    encoder.string(SERVER_STORE_NAME_FIELD, message.getName());
    configCodec.encodeServerStoreConfiguration(encoder, message.getStoreConfiguration());
    return encoder.encode().array();
  }

  private byte[] encodeTierManagerConfigureMessage(LifecycleMessage.ConfigureStoreManager message) {
    return encodeTierManagerCreateOrValidate(message, message.getConfiguration(), configureMessageStruct.encoder());
  }

  private byte[] encodeTierManagerValidateMessage(LifecycleMessage.ValidateStoreManager message) {
    return encodeTierManagerCreateOrValidate(message, message.getConfiguration(), validateMessageStruct.encoder());
  }

  private byte[] encodeTierManagerCreateOrValidate(LifecycleMessage message, ServerSideConfiguration config, StructEncoder<Void> encoder) {
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
      case CONFIGURE:
        return decodeConfigureMessage(messageBuffer);
      case VALIDATE:
        return decodeValidateMessage(messageBuffer);
      case CREATE_SERVER_STORE:
        return decodeCreateServerStoreMessage(messageBuffer);
      case VALIDATE_SERVER_STORE:
        return decodeValidateServerStoreMessage(messageBuffer);
      case DESTROY_SERVER_STORE:
        return decodeDestroyServerStoreMessage(messageBuffer);
      case RELEASE_SERVER_STORE:
        return decodeReleaseServerStoreMessage(messageBuffer);
    }
    throw new IllegalArgumentException("LifeCycleMessage operation not defined for : " + messageType);
  }

  private LifecycleMessage.ReleaseServerStore decodeReleaseServerStoreMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = RELEASE_STORE_MESSAGE_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID cliendId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);

    LifecycleMessage.ReleaseServerStore message = new LifecycleMessage.ReleaseServerStore(storeName, cliendId);
    message.setId(msgId);
    return message;
  }

  private LifecycleMessage.DestroyServerStore decodeDestroyServerStoreMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = DESTROY_STORE_MESSAGE_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID cliendId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);

    LifecycleMessage.DestroyServerStore message = new LifecycleMessage.DestroyServerStore(storeName, cliendId);
    message.setId(msgId);
    return message;
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

  private LifecycleMessage.CreateServerStore decodeCreateServerStoreMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = createStoreMessageStruct.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID cliendId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);
    ServerStoreConfiguration config = configCodec.decodeServerStoreConfiguration(decoder);

    LifecycleMessage.CreateServerStore message = new LifecycleMessage.CreateServerStore(storeName, config, cliendId);
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

  private LifecycleMessage.ConfigureStoreManager decodeConfigureMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = configureMessageStruct.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID clientId = messageCodecUtils.decodeUUID(decoder);
    boolean configPresent = decoder.bool(CONFIG_PRESENT_FIELD);

    ServerSideConfiguration config = null;
    if (configPresent) {
      config = configCodec.decodeServerSideConfiguration(decoder);
    }

    LifecycleMessage.ConfigureStoreManager message = new LifecycleMessage.ConfigureStoreManager(config, clientId);
    if (msgId != null) {
      message.setId(msgId);
    }
    return message;
  }
}
