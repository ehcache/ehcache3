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

import org.ehcache.clustered.common.internal.messages.ChainCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.store.Chain;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.StructDecoder;

import java.nio.ByteBuffer;

import static org.ehcache.clustered.common.internal.messages.BaseCodec.EHCACHE_MESSAGE_TYPES_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.BaseCodec.MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.common.internal.messages.BaseCodec.MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.KEY_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.SERVER_STORE_NAME_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.encodeMandatoryFields;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class PassiveReplicationMessageCodec {

  private static final String CHAIN_FIELD = "chain";
  private static final String OLDEST_TRANSACTION_ID_FIELD = "otId";
  private static final String TRANSACTION_ID_FIELD = "tId";
  private static final String CLIENT_ID_FIELD = "cId";

  private static final Struct CHAIN_REPLICATION_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(TRANSACTION_ID_FIELD, 15)
    .int64(CLIENT_ID_FIELD, 20)
    .string(SERVER_STORE_NAME_FIELD, 30)
    .int64(OLDEST_TRANSACTION_ID_FIELD, 35)
    .int64(KEY_FIELD, 40)
    .struct(CHAIN_FIELD, 45, ChainCodec.CHAIN_STRUCT)
    .build();

  private static final Struct CLEAR_INVALIDATION_COMPLETE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .build();

  private static final Struct INVALIDATION_COMPLETE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(KEY_FIELD, 20)
    .build();

  public byte[] encode(PassiveReplicationMessage message) {
    switch (message.getMessageType()) {
      case CHAIN_REPLICATION_OP:
        return encodeChainReplicationMessage((PassiveReplicationMessage.ChainReplicationMessage) message);
      case CLEAR_INVALIDATION_COMPLETE:
        return encodeClearInvalidationCompleteMessage((PassiveReplicationMessage.ClearInvalidationCompleteMessage) message);
      case INVALIDATION_COMPLETE:
        return encodeInvalidationCompleteMessage((PassiveReplicationMessage.InvalidationCompleteMessage) message);
      default:
        throw new UnsupportedOperationException("This operation is not supported : " + message.getMessageType());
    }
  }

  private byte[] encodeInvalidationCompleteMessage(PassiveReplicationMessage.InvalidationCompleteMessage message) {
    return encodeMandatoryFields(INVALIDATION_COMPLETE_STRUCT, message)
      .int64(KEY_FIELD, message.getKey())
      .encode().array();
  }

  private byte[] encodeClearInvalidationCompleteMessage(PassiveReplicationMessage.ClearInvalidationCompleteMessage message) {
    return encodeMandatoryFields(CLEAR_INVALIDATION_COMPLETE_STRUCT, message)
      .encode().array();
  }

  private byte[] encodeChainReplicationMessage(PassiveReplicationMessage.ChainReplicationMessage message) {
    return encodeMandatoryFields(CHAIN_REPLICATION_STRUCT, message)
      .int64(TRANSACTION_ID_FIELD, message.getTransactionId())
      .int64(CLIENT_ID_FIELD, message.getClientId())
      .int64(OLDEST_TRANSACTION_ID_FIELD, message.getOldestTransactionId())
      .int64(KEY_FIELD, message.getKey())
      .struct(CHAIN_FIELD, message.getChain(), ChainCodec::encode)
      .encode().array();
  }

  public EhcacheEntityMessage decode(EhcacheMessageType messageType, ByteBuffer messageBuffer) {

    switch (messageType) {
      case CHAIN_REPLICATION_OP:
        return decodeChainReplicationMessage(messageBuffer);
      case CLEAR_INVALIDATION_COMPLETE:
        return decodeClearInvalidationCompleteMessage(messageBuffer);
      case INVALIDATION_COMPLETE:
        return decodeInvalidationCompleteMessage(messageBuffer);
      default:
        throw new UnsupportedOperationException("Unknown message type: " + messageType);
    }
  }

  private PassiveReplicationMessage.InvalidationCompleteMessage decodeInvalidationCompleteMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = INVALIDATION_COMPLETE_STRUCT.decoder(messageBuffer);

    Long key = decoder.int64(KEY_FIELD);

    return new PassiveReplicationMessage.InvalidationCompleteMessage(key);
  }

  private PassiveReplicationMessage.ClearInvalidationCompleteMessage decodeClearInvalidationCompleteMessage(ByteBuffer messageBuffer) {
    return new PassiveReplicationMessage.ClearInvalidationCompleteMessage();
  }

  private PassiveReplicationMessage.ChainReplicationMessage decodeChainReplicationMessage(ByteBuffer messageBuffer) {
    StructDecoder<Void> decoder = CHAIN_REPLICATION_STRUCT.decoder(messageBuffer);

    Long currentTransactionId = decoder.int64(TRANSACTION_ID_FIELD);
    Long clientId = decoder.int64(CLIENT_ID_FIELD);

    Long oldestTransactionId = decoder.int64(OLDEST_TRANSACTION_ID_FIELD);
    Long key = decoder.int64(KEY_FIELD);

    Chain chain = ChainCodec.decode(decoder.struct(CHAIN_FIELD));

    return new PassiveReplicationMessage.ChainReplicationMessage(key, chain, currentTransactionId, oldestTransactionId, clientId);
  }

}
