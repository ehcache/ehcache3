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
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructArrayEncoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.EHCACHE_MESSAGE_TYPES_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.CONSISTENCY_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.DEFAULT_RESOURCE_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.LSB_UUID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.MSB_UUID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.MSG_ID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.POOLS_SUB_STRUCT;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.POOL_NAME_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.POOL_RESOURCE_NAME_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.POOL_SIZE_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.SERVER_STORE_NAME_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.STORE_CONFIG_CONSISTENCY_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.STORE_CONFIG_KEY_SERIALIZER_TYPE_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.STORE_CONFIG_KEY_TYPE_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.STORE_CONFIG_VALUE_SERIALIZER_TYPE_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.STORE_CONFIG_VALUE_TYPE_FIELD;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

class LifeCycleMessageCodec {

  private static final String CONFIG_PRESENT_FIELD = "configPresent";

  private static final Struct POOLS_STRUCT = newStructBuilder()
    .string(POOL_NAME_FIELD, 10)
    .int64(POOL_SIZE_FIELD, 20)
    .string(POOL_RESOURCE_NAME_FIELD, 30).build();

  private static final Struct CONFIGURE_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .bool(CONFIG_PRESENT_FIELD, 30)
    .string(DEFAULT_RESOURCE_FIELD, 40)
    .structs(POOLS_SUB_STRUCT, 50, POOLS_STRUCT)
    .build();

  private static final Struct VALIDATE_MESSAGE_STRUCT = CONFIGURE_MESSAGE_STRUCT;

  private static final Struct CREATE_STORE_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .string(STORE_CONFIG_KEY_TYPE_FIELD, 40)
    .string(STORE_CONFIG_KEY_SERIALIZER_TYPE_FIELD, 41)
    .string(STORE_CONFIG_VALUE_TYPE_FIELD, 45)
    .string(STORE_CONFIG_VALUE_SERIALIZER_TYPE_FIELD, 46)
    .enm(STORE_CONFIG_CONSISTENCY_FIELD, 50, CONSISTENCY_ENUM_MAPPING)
    .int64(POOL_SIZE_FIELD, 60)
    .string(POOL_RESOURCE_NAME_FIELD, 65)
    .build();

  private static final Struct VALIDATE_STORE_MESSAGE_STRUCT = CREATE_STORE_MESSAGE_STRUCT;

  private static final Struct DESTROY_STORE_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .build();

  private static final Struct RELEASE_STORE_MESSAGE_STRUCTU = DESTROY_STORE_MESSAGE_STRUCT;

  private final MessageCodecUtils messageCodecUtils = new MessageCodecUtils();

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
    StructEncoder encoder = RELEASE_STORE_MESSAGE_STRUCTU.encoder();

    messageCodecUtils.encodeMandatoryFields(encoder, message);
    encoder.string(SERVER_STORE_NAME_FIELD, message.getName());
    return encoder.encode().array();
  }

  private byte[] encodeDestroyStoreMessage(LifecycleMessage.DestroyServerStore message) {
    StructEncoder encoder = DESTROY_STORE_MESSAGE_STRUCT.encoder();

    messageCodecUtils.encodeMandatoryFields(encoder, message);
    encoder.string(SERVER_STORE_NAME_FIELD, message.getName());
    return encoder.encode().array();
  }

  private byte[] encodeCreateStoreMessage(LifecycleMessage.CreateServerStore message) {
    StructEncoder encoder = CREATE_STORE_MESSAGE_STRUCT.encoder();
    return encodeBaseServerStoreMessage(message, encoder);
  }

  private byte[] encodeValidateStoreMessage(LifecycleMessage.ValidateServerStore message) {
    return encodeBaseServerStoreMessage(message, VALIDATE_STORE_MESSAGE_STRUCT.encoder());
  }

  private byte[] encodeBaseServerStoreMessage(LifecycleMessage.BaseServerStore message, StructEncoder encoder) {
    messageCodecUtils.encodeMandatoryFields(encoder, message);

    encoder.string(SERVER_STORE_NAME_FIELD, message.getName());
    messageCodecUtils.encodeServerStoreConfiguration(encoder, message.getStoreConfiguration());
    return encoder.encode().array();
  }

  private byte[] encodeTierManagerConfigureMessage(LifecycleMessage.ConfigureStoreManager message) {
    return encodeTierManagerCreateOrValidate(message, message.getConfiguration(), CONFIGURE_MESSAGE_STRUCT.encoder());
  }

  private byte[] encodeTierManagerValidateMessage(LifecycleMessage.ValidateStoreManager message) {
    return encodeTierManagerCreateOrValidate(message, message.getConfiguration(), VALIDATE_MESSAGE_STRUCT.encoder());
  }

  private byte[] encodeTierManagerCreateOrValidate(LifecycleMessage message, ServerSideConfiguration config, StructEncoder encoder) {
    messageCodecUtils.encodeMandatoryFields(encoder, message);
    encodeServerSideConfiguration(encoder, config);
    return encoder.encode().array();
  }

  private void encodeServerSideConfiguration(StructEncoder encoder, ServerSideConfiguration configuration) {
    if (configuration == null) {
      encoder.bool(CONFIG_PRESENT_FIELD, false);
    } else {
      encoder.bool(CONFIG_PRESENT_FIELD, true);
      if (configuration.getDefaultServerResource() != null) {
        encoder.string(DEFAULT_RESOURCE_FIELD, configuration.getDefaultServerResource());
      }

      if (!configuration.getResourcePools().isEmpty()) {
        StructArrayEncoder poolsEncoder = encoder.structs(POOLS_SUB_STRUCT);
        for (Map.Entry<String, ServerSideConfiguration.Pool> poolEntry : configuration.getResourcePools().entrySet()) {
          poolsEncoder.string(POOL_NAME_FIELD, poolEntry.getKey())
            .int64(POOL_SIZE_FIELD, poolEntry.getValue().getSize());
          if (poolEntry.getValue().getServerResource() != null) {
            poolsEncoder.string(POOL_RESOURCE_NAME_FIELD, poolEntry.getValue().getServerResource());
          }
          poolsEncoder.next();
        }
        poolsEncoder.end();
      }
    }
  }

  private ServerSideConfiguration decodeServerSideConfiguration(StructDecoder decoder) {
    boolean configPresent = decoder.bool(CONFIG_PRESENT_FIELD);

    if (configPresent) {
      String defaultResource = decoder.string(DEFAULT_RESOURCE_FIELD);

      HashMap<String, ServerSideConfiguration.Pool> resourcePools = new HashMap<String, ServerSideConfiguration.Pool>();
      StructArrayDecoder poolStructs = decoder.structs(POOLS_SUB_STRUCT);
      if (poolStructs != null) {
        for (int i = 0; i < poolStructs.length(); i++) {
          String poolName = poolStructs.string(POOL_NAME_FIELD);
          Long poolSize = poolStructs.int64(POOL_SIZE_FIELD);
          String poolResourceName = poolStructs.string(POOL_RESOURCE_NAME_FIELD);
          if (poolResourceName == null) {
            resourcePools.put(poolName, new ServerSideConfiguration.Pool(poolSize));
          } else {
            resourcePools.put(poolName, new ServerSideConfiguration.Pool(poolSize, poolResourceName));
          }
          poolStructs.next();
        }
      }

      ServerSideConfiguration serverSideConfiguration;
      if (defaultResource == null) {
        serverSideConfiguration = new ServerSideConfiguration(resourcePools);
      } else {
        serverSideConfiguration = new ServerSideConfiguration(defaultResource, resourcePools);
      }
      return serverSideConfiguration;
    } else {
      return null;
    }
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
    StructDecoder decoder = RELEASE_STORE_MESSAGE_STRUCTU.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID cliendId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);

    LifecycleMessage.ReleaseServerStore message = new LifecycleMessage.ReleaseServerStore(storeName, cliendId);
    message.setId(msgId);
    return message;
  }

  private LifecycleMessage.DestroyServerStore decodeDestroyServerStoreMessage(ByteBuffer messageBuffer) {
    StructDecoder decoder = DESTROY_STORE_MESSAGE_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID cliendId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);

    LifecycleMessage.DestroyServerStore message = new LifecycleMessage.DestroyServerStore(storeName, cliendId);
    message.setId(msgId);
    return message;
  }

  private LifecycleMessage.ValidateServerStore decodeValidateServerStoreMessage(ByteBuffer messageBuffer) {
    StructDecoder decoder = VALIDATE_STORE_MESSAGE_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID cliendId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);
    ServerStoreConfiguration config = messageCodecUtils.decodeServerStoreConfiguration(decoder);

    LifecycleMessage.ValidateServerStore message = new LifecycleMessage.ValidateServerStore(storeName, config, cliendId);
    message.setId(msgId);
    return message;
  }

  private LifecycleMessage.CreateServerStore decodeCreateServerStoreMessage(ByteBuffer messageBuffer) {
    StructDecoder decoder = CREATE_STORE_MESSAGE_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID cliendId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);
    ServerStoreConfiguration config = messageCodecUtils.decodeServerStoreConfiguration(decoder);

    LifecycleMessage.CreateServerStore message = new LifecycleMessage.CreateServerStore(storeName, config, cliendId);
    message.setId(msgId);
    return message;
  }

  private LifecycleMessage.ValidateStoreManager decodeValidateMessage(ByteBuffer messageBuffer) {
    StructDecoder decoder = VALIDATE_MESSAGE_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID cliendId = messageCodecUtils.decodeUUID(decoder);

    ServerSideConfiguration config = decodeServerSideConfiguration(decoder);

    LifecycleMessage.ValidateStoreManager message = new LifecycleMessage.ValidateStoreManager(config, cliendId);
    if (msgId != null) {
      message.setId(msgId);
    }
    return message;
  }

  private LifecycleMessage.ConfigureStoreManager decodeConfigureMessage(ByteBuffer messageBuffer) {
    StructDecoder decoder = CONFIGURE_MESSAGE_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID clientId = messageCodecUtils.decodeUUID(decoder);

    ServerSideConfiguration config = decodeServerSideConfiguration(decoder);

    LifecycleMessage.ConfigureStoreManager message = new LifecycleMessage.ConfigureStoreManager(config, clientId);
    if (msgId != null) {
      message.setId(msgId);
    }
    return message;
  }

}
