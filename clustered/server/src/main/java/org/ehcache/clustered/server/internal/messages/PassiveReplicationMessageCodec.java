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

import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.ChainCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.MessageCodecUtils;
import org.ehcache.clustered.common.internal.store.Chain;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.EHCACHE_MESSAGE_TYPES_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.CONSISTENCY_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.KEY_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.LSB_UUID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.MSB_UUID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.MSG_ID_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.POOL_RESOURCE_NAME_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.POOL_SIZE_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.SERVER_STORE_NAME_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.STORE_CONFIG_CONSISTENCY_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.STORE_CONFIG_KEY_SERIALIZER_TYPE_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.STORE_CONFIG_KEY_TYPE_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.STORE_CONFIG_VALUE_SERIALIZER_TYPE_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.STORE_CONFIG_VALUE_TYPE_FIELD;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class PassiveReplicationMessageCodec {

  private static final String CHAIN_FIELD = "chain";

  private static final Struct CLIENT_ID_TRACK_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .build();

  private static final Struct CHAIN_REPLICATION_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .int64(KEY_FIELD, 40)
    .struct(CHAIN_FIELD, 45, ChainCodec.CHAIN_STRUCT)
    .build();

  private static final Struct CLEAR_INVALIDATION_COMPLETE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .string(SERVER_STORE_NAME_FIELD, 20)
    .build();

  private static final Struct INVALIDATION_COMPLETE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .string(SERVER_STORE_NAME_FIELD, 20)
    .int64(KEY_FIELD, 30)
    .build();

  private static final Struct CREATE_SERVER_STORE_REPLICATION_STRUCT = newStructBuilder()
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

  private static final Struct DESTROY_SERVER_STORE_REPLICATION_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(MSG_ID_FIELD, 15)
    .int64(MSB_UUID_FIELD, 20)
    .int64(LSB_UUID_FIELD, 21)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .build();

  private final ChainCodec chainCodec = new ChainCodec();
  private final MessageCodecUtils messageCodecUtils = new MessageCodecUtils();

  public byte[] encode(PassiveReplicationMessage message) {

    switch (message.getMessageType()) {
      case CLIENT_ID_TRACK_OP:
        return encodeClientIdTrackMessage((PassiveReplicationMessage.ClientIDTrackerMessage) message);
      case CHAIN_REPLICATION_OP:
        return encodeChainReplicationMessage((PassiveReplicationMessage.ChainReplicationMessage) message);
      case CLEAR_INVALIDATION_COMPLETE:
        return encodeClearInvalidationCompleteMessage((PassiveReplicationMessage.ClearInvalidationCompleteMessage) message);
      case INVALIDATION_COMPLETE:
        return encodeInvalidationCompleteMessage((PassiveReplicationMessage.InvalidationCompleteMessage) message);
      case CREATE_SERVER_STORE_REPLICATION:
        return encodeCreateServerStoreReplicationMessage((PassiveReplicationMessage.CreateServerStoreReplicationMessage) message);
      case DESTROY_SERVER_STORE_REPLICATION:
        return encoreDestroyServerStoreReplicationMessage((PassiveReplicationMessage.DestroyServerStoreReplicationMessage) message);
      default:
        throw new UnsupportedOperationException("This operation is not supported : " + message.getMessageType());
    }
  }

  private byte[] encoreDestroyServerStoreReplicationMessage(PassiveReplicationMessage.DestroyServerStoreReplicationMessage message) {
    StructEncoder encoder = DESTROY_SERVER_STORE_REPLICATION_STRUCT.encoder();

    messageCodecUtils.encodeMandatoryFields(encoder, message);
    encoder.string(SERVER_STORE_NAME_FIELD, message.getStoreName());

    return encoder.encode().array();
  }

  private byte[] encodeCreateServerStoreReplicationMessage(PassiveReplicationMessage.CreateServerStoreReplicationMessage message) {
    StructEncoder encoder = CREATE_SERVER_STORE_REPLICATION_STRUCT.encoder();

    messageCodecUtils.encodeMandatoryFields(encoder, message);
    encoder.string(SERVER_STORE_NAME_FIELD, message.getStoreName());
    messageCodecUtils.encodeServerStoreConfiguration(encoder, message.getStoreConfiguration());

    return encoder.encode().array();
  }

  private byte[] encodeInvalidationCompleteMessage(PassiveReplicationMessage.InvalidationCompleteMessage message) {
    StructEncoder encoder = INVALIDATION_COMPLETE_STRUCT.encoder();

    encoder.enm(MESSAGE_TYPE_FIELD_NAME, message.getMessageType())
      .string(SERVER_STORE_NAME_FIELD, message.getCacheId())
      .int64(KEY_FIELD, message.getKey());

    return encoder.encode().array();
  }

  private byte[] encodeClearInvalidationCompleteMessage(PassiveReplicationMessage.ClearInvalidationCompleteMessage message) {
    StructEncoder encoder = CLEAR_INVALIDATION_COMPLETE_STRUCT.encoder();

    encoder.enm(MESSAGE_TYPE_FIELD_NAME, message.getMessageType())
      .string(SERVER_STORE_NAME_FIELD, message.getCacheId());

    return encoder.encode().array();
  }

  private byte[] encodeChainReplicationMessage(PassiveReplicationMessage.ChainReplicationMessage message) {
    StructEncoder encoder = CHAIN_REPLICATION_STRUCT.encoder();

    messageCodecUtils.encodeMandatoryFields(encoder, message);

    encoder.string(SERVER_STORE_NAME_FIELD, message.getCacheId());
    encoder.int64(KEY_FIELD, message.getKey());
    chainCodec.encode(encoder.struct(CHAIN_FIELD), message.getChain());

    return encoder.encode().array();
  }

  private byte[] encodeClientIdTrackMessage(PassiveReplicationMessage.ClientIDTrackerMessage message) {
    StructEncoder encoder = CLIENT_ID_TRACK_STRUCT.encoder();

    encoder.enm(EhcacheMessageType.MESSAGE_TYPE_FIELD_NAME, message.getMessageType())
        .int64(MSB_UUID_FIELD, message.getClientId().getMostSignificantBits())
        .int64(LSB_UUID_FIELD, message.getClientId().getLeastSignificantBits());

    return encoder.encode().array();
  }

  public EhcacheEntityMessage decode(EhcacheMessageType messageType, ByteBuffer messageBuffer) {

    switch (messageType) {
      case CLIENT_ID_TRACK_OP:
        return decodeClientIdTrackMessage(messageBuffer);
      case CHAIN_REPLICATION_OP:
        return decodeChainReplicationMessage(messageBuffer);
      case CLEAR_INVALIDATION_COMPLETE:
        return decodeClearInvalidationCompleteMessage(messageBuffer);
      case INVALIDATION_COMPLETE:
        return decodeInvalidationCompleteMessage(messageBuffer);
      case CREATE_SERVER_STORE_REPLICATION:
        return decodeCreateServerStoreReplicationMessage(messageBuffer);
      case DESTROY_SERVER_STORE_REPLICATION:
        return decodeDestroyServerStoreReplicationMessage(messageBuffer);
      default:
        throw new UnsupportedOperationException("Unknown message type: " + messageType);
    }
  }

  private PassiveReplicationMessage.DestroyServerStoreReplicationMessage decodeDestroyServerStoreReplicationMessage(ByteBuffer messageBuffer) {
    StructDecoder decoder = DESTROY_SERVER_STORE_REPLICATION_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID clientId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);

    return new PassiveReplicationMessage.DestroyServerStoreReplicationMessage(msgId, clientId, storeName);
  }

  private PassiveReplicationMessage.CreateServerStoreReplicationMessage decodeCreateServerStoreReplicationMessage(ByteBuffer messageBuffer) {
    StructDecoder decoder = CREATE_SERVER_STORE_REPLICATION_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID clientId = messageCodecUtils.decodeUUID(decoder);

    String storeName = decoder.string(SERVER_STORE_NAME_FIELD);
    ServerStoreConfiguration configuration = messageCodecUtils.decodeServerStoreConfiguration(decoder);

    return new PassiveReplicationMessage.CreateServerStoreReplicationMessage(msgId, clientId, storeName, configuration);
  }

  private PassiveReplicationMessage.InvalidationCompleteMessage decodeInvalidationCompleteMessage(ByteBuffer messageBuffer) {
    StructDecoder decoder = INVALIDATION_COMPLETE_STRUCT.decoder(messageBuffer);

    String storeId = decoder.string(SERVER_STORE_NAME_FIELD);
    Long key = decoder.int64(KEY_FIELD);

    return new PassiveReplicationMessage.InvalidationCompleteMessage(storeId, key);
  }

  private PassiveReplicationMessage.ClearInvalidationCompleteMessage decodeClearInvalidationCompleteMessage(ByteBuffer messageBuffer) {
    StructDecoder decoder = CLEAR_INVALIDATION_COMPLETE_STRUCT.decoder(messageBuffer);
    return new PassiveReplicationMessage.ClearInvalidationCompleteMessage(decoder.string(SERVER_STORE_NAME_FIELD));
  }

  private PassiveReplicationMessage.ChainReplicationMessage decodeChainReplicationMessage(ByteBuffer messageBuffer) {
    StructDecoder decoder = CHAIN_REPLICATION_STRUCT.decoder(messageBuffer);

    Long msgId = decoder.int64(MSG_ID_FIELD);
    UUID clientId = messageCodecUtils.decodeUUID(decoder);

    String cacheId = decoder.string(SERVER_STORE_NAME_FIELD);
    Long key = decoder.int64(KEY_FIELD);

    Chain chain = chainCodec.decode(decoder.struct(CHAIN_FIELD));

    return new PassiveReplicationMessage.ChainReplicationMessage(cacheId, key, chain, msgId, clientId);
  }

  private PassiveReplicationMessage.ClientIDTrackerMessage decodeClientIdTrackMessage(ByteBuffer messageBuffer) {
    StructDecoder decoder = CLIENT_ID_TRACK_STRUCT.decoder(messageBuffer);

    UUID clientId = messageCodecUtils.decodeUUID(decoder);

    return new PassiveReplicationMessage.ClientIDTrackerMessage(clientId);
  }

}
