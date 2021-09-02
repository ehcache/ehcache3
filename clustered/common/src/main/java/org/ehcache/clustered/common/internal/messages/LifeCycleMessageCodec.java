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

import static org.ehcache.clustered.common.internal.messages.BaseCodec.EHCACHE_MESSAGE_TYPES_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.BaseCodec.MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.common.internal.messages.BaseCodec.MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.SERVER_STORE_NAME_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.encodeMandatoryFields;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class LifeCycleMessageCodec {

  private static final String CONFIG_PRESENT_FIELD = "configPresent";

  private static final int CONFIGURE_MESSAGE_NEXT_INDEX = 40;
  private static final int VALIDATE_STORE_NEXT_INDEX = 40;

  private final Struct PREPARE_FOR_DESTROY_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .build();

  private final Struct validateMessageStruct;
  private final Struct validateStoreMessageStruct;

  private final ConfigCodec configCodec;

  public LifeCycleMessageCodec(ConfigCodec configCodec) {
    this.configCodec = configCodec;

    StructBuilder validateMessageStructBuilderPrefix = newStructBuilder()
      .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
      .bool(CONFIG_PRESENT_FIELD, 30);

    validateMessageStruct = this.configCodec.injectServerSideConfiguration(
      validateMessageStructBuilderPrefix, CONFIGURE_MESSAGE_NEXT_INDEX).getUpdatedBuilder().build();

    StructBuilder validateStoreMessageStructBuilderPrefix = newStructBuilder()
      .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
      .string(SERVER_STORE_NAME_FIELD, 30);

    validateStoreMessageStruct = this.configCodec.injectServerStoreConfiguration(
      validateStoreMessageStructBuilderPrefix, VALIDATE_STORE_NEXT_INDEX).getUpdatedBuilder().build();
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
    return encodeMandatoryFields(PREPARE_FOR_DESTROY_STRUCT, message).encode().array();
  }

  private byte[] encodeValidateStoreMessage(LifecycleMessage.ValidateServerStore message) {
    StructEncoder<Void> encoder = encodeMandatoryFields(validateStoreMessageStruct, message);

    encoder.string(SERVER_STORE_NAME_FIELD, message.getName());
    configCodec.encodeServerStoreConfiguration(encoder, message.getStoreConfiguration());
    return encoder.encode().array();
  }

  private byte[] encodeTierManagerValidateMessage(LifecycleMessage.ValidateStoreManager message) {
    StructEncoder<Void> encoder = encodeMandatoryFields(validateMessageStruct, message);
    ServerSideConfiguration config = message.getConfiguration();
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
      default:
        throw new IllegalArgumentException("LifeCycleMessage operation not defined for : " + messageType);
    }
  }

  private LifecycleMessage.PrepareForDestroy decodePrepareForDestroyMessage() {
    return new LifecycleMessage.PrepareForDestroy();
  }

  private LifecycleMessage.ValidateServerStore decodeValidateServerStoreMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = validateStoreMessageStruct.decoder(messageBuffer);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);
    ServerStoreConfiguration config = configCodec.decodeServerStoreConfiguration(decoder);

    return new LifecycleMessage.ValidateServerStore(storeName, config);
  }

  private LifecycleMessage.ValidateStoreManager decodeValidateMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = validateMessageStruct.decoder(messageBuffer);

    boolean configPresent = decoder.bool(CONFIG_PRESENT_FIELD);

    ServerSideConfiguration config = null;
    if (configPresent) {
      config = configCodec.decodeServerSideConfiguration(decoder);
    }

    return new LifecycleMessage.ValidateStoreManager(config);
  }
}
