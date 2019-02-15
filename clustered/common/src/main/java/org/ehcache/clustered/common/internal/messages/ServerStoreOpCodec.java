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

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.ehcache.clustered.common.internal.messages.ChainCodec.CHAIN_STRUCT;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.EHCACHE_MESSAGE_TYPES_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.KEY_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.encodeMandatoryFields;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class ServerStoreOpCodec {

  private static final Struct GET_AND_APPEND_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(KEY_FIELD, 30)
    .byteBuffer("payload", 40)
    .build();

  private static final Struct APPEND_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(KEY_FIELD, 30)
    .byteBuffer("payload", 40)
    .build();

  private static final Struct REPLACE_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(KEY_FIELD, 30)
    .struct("expect", 40, CHAIN_STRUCT)
    .struct("update", 50, CHAIN_STRUCT)
    .build();

  private static final Struct CLIENT_INVALIDATION_ACK_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(KEY_FIELD, 20)
    .int32("invalidationId", 30)
    .build();

  private static final Struct CLIENT_INVALIDATION_ALL_ACK_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int32("invalidationId", 40)
    .build();

  private static final Struct CLEAR_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .build();

  private static final Struct GET_MESSAGE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64(KEY_FIELD, 30)
    .build();

  private static final Struct LOCK_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int64("hash", 30)
    .build();

  private static final Struct ITERATOR_OPEN_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .int32("batchSize", 20)
    .build();

  private static final Struct ITERATOR_CLOSE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .string("id", 20)
    .build();

  private static final Struct ITERATOR_ADVANCE_STRUCT = newStructBuilder()
    .enm(MESSAGE_TYPE_FIELD_NAME, MESSAGE_TYPE_FIELD_INDEX, EHCACHE_MESSAGE_TYPES_ENUM_MAPPING)
    .string("id", 20)
    .int32("batchSize", 30)
    .build();

  public byte[] encode(ServerStoreOpMessage message) {
    switch (message.getMessageType()) {
      case GET_STORE:
        GetMessage getMessage = (GetMessage) message;
        return encodeMandatoryFields(GET_MESSAGE_STRUCT, message)
          .int64(KEY_FIELD, getMessage.getKey())
          .encode().array();
      case APPEND:
        AppendMessage appendMessage = (AppendMessage) message;
        return encodeMandatoryFields(APPEND_MESSAGE_STRUCT, message)
          .int64(KEY_FIELD, appendMessage.getKey())
          .byteBuffer("payload", appendMessage.getPayload())
          .encode().array();
      case GET_AND_APPEND:
        GetAndAppendMessage getAndAppendMessage = (GetAndAppendMessage) message;
        return encodeMandatoryFields(GET_AND_APPEND_MESSAGE_STRUCT, message)
          .int64(KEY_FIELD, getAndAppendMessage.getKey())
          .byteBuffer("payload", getAndAppendMessage.getPayload())
          .encode().array();
      case REPLACE:
        final ReplaceAtHeadMessage replaceAtHeadMessage = (ReplaceAtHeadMessage) message;
        return encodeMandatoryFields(REPLACE_MESSAGE_STRUCT, message)
          .int64(KEY_FIELD, replaceAtHeadMessage.getKey())
          .struct("expect", replaceAtHeadMessage.getExpect(), ChainCodec::encode)
          .struct("update", replaceAtHeadMessage.getUpdate(), ChainCodec::encode)
          .encode().array();
      case CLIENT_INVALIDATION_ACK:
        ClientInvalidationAck clientInvalidationAckMessage = (ClientInvalidationAck) message;
        return encodeMandatoryFields(CLIENT_INVALIDATION_ACK_MESSAGE_STRUCT, message)
          .int64(KEY_FIELD, clientInvalidationAckMessage.getKey())
          .int32("invalidationId", clientInvalidationAckMessage.getInvalidationId())
          .encode().array();
      case CLIENT_INVALIDATION_ALL_ACK:
        ClientInvalidationAllAck clientInvalidationAllAckMessage = (ClientInvalidationAllAck) message;
        return encodeMandatoryFields(CLIENT_INVALIDATION_ALL_ACK_MESSAGE_STRUCT, message)
          .int32("invalidationId", clientInvalidationAllAckMessage.getInvalidationId())
          .encode().array();
      case CLEAR:
        return encodeMandatoryFields(CLEAR_MESSAGE_STRUCT, message)
          .encode().array();
      case LOCK:
        ServerStoreOpMessage.LockMessage lockMessage = (ServerStoreOpMessage.LockMessage) message;
        return encodeMandatoryFields(LOCK_STRUCT, message)
          .int64("hash", lockMessage.getHash())
          .encode().array();
      case UNLOCK:
        ServerStoreOpMessage.UnlockMessage unlockMessage = (ServerStoreOpMessage.UnlockMessage) message;
        return encodeMandatoryFields(LOCK_STRUCT, message)
          .int64("hash", unlockMessage.getHash())
          .encode().array();
      case ITERATOR_OPEN:
        return encodeMandatoryFields(ITERATOR_OPEN_STRUCT, message)
          .int32("batchSize", ((ServerStoreOpMessage.IteratorOpenMessage) message).getBatchSize())
          .encode().array();
      case ITERATOR_CLOSE:
        return encodeMandatoryFields(ITERATOR_CLOSE_STRUCT, message)
          .string("id", ((ServerStoreOpMessage.IteratorCloseMessage) message).getIdentity().toString())
          .encode().array();
      case ITERATOR_ADVANCE:
        return encodeMandatoryFields(ITERATOR_ADVANCE_STRUCT, message)
          .string("id", ((ServerStoreOpMessage.IteratorAdvanceMessage) message).getIdentity().toString())
          .int32("batchSize", ((ServerStoreOpMessage.IteratorAdvanceMessage) message).getBatchSize())
          .encode().array();
      default:
        throw new RuntimeException("Unhandled message operation : " + message.getMessageType());
    }
  }

  public EhcacheEntityMessage decode(EhcacheMessageType opCode, ByteBuffer messageBuffer) {
    switch (opCode) {
      case GET_STORE: {
        StructDecoder<Void> decoder = GET_MESSAGE_STRUCT.decoder(messageBuffer);
        Long key = decoder.int64(KEY_FIELD);
        return new GetMessage(key);
      }
      case GET_AND_APPEND: {
        StructDecoder<Void> decoder = GET_AND_APPEND_MESSAGE_STRUCT.decoder(messageBuffer);
        Long key = decoder.int64(KEY_FIELD);
        ByteBuffer payload = decoder.byteBuffer("payload");
        return new GetAndAppendMessage(key, payload);
      }
      case APPEND: {
        StructDecoder<Void> decoder = APPEND_MESSAGE_STRUCT.decoder(messageBuffer);
        Long key = decoder.int64(KEY_FIELD);
        ByteBuffer payload = decoder.byteBuffer("payload");
        return new AppendMessage(key, payload);
      }
      case REPLACE: {
        StructDecoder<Void> decoder = REPLACE_MESSAGE_STRUCT.decoder(messageBuffer);
        Long key = decoder.int64(KEY_FIELD);
        Chain expect = ChainCodec.decode(decoder.struct("expect"));
        Chain update = ChainCodec.decode(decoder.struct("update"));
        return new ReplaceAtHeadMessage(key, expect, update);
      }
      case CLIENT_INVALIDATION_ACK: {
        StructDecoder<Void> decoder = CLIENT_INVALIDATION_ACK_MESSAGE_STRUCT.decoder(messageBuffer);
        Long key = decoder.int64(KEY_FIELD);
        Integer invalidationId = decoder.int32("invalidationId");
        return new ClientInvalidationAck(key, invalidationId);
      }
      case CLIENT_INVALIDATION_ALL_ACK: {
        StructDecoder<Void> decoder = CLIENT_INVALIDATION_ALL_ACK_MESSAGE_STRUCT.decoder(messageBuffer);
        Integer invalidationId = decoder.int32("invalidationId");
        return new ClientInvalidationAllAck(invalidationId);
      }
      case CLEAR: {
        return new ClearMessage();
      }
      case LOCK: {
        StructDecoder<Void> decoder = LOCK_STRUCT.decoder(messageBuffer);
        long hash = decoder.int64("hash");
        return new ServerStoreOpMessage.LockMessage(hash);
      }
      case UNLOCK: {
        StructDecoder<Void> decoder = LOCK_STRUCT.decoder(messageBuffer);
        long hash = decoder.int64("hash");
        return new ServerStoreOpMessage.UnlockMessage(hash);
      }
      case ITERATOR_OPEN: {
        StructDecoder<Void> decoder = ITERATOR_OPEN_STRUCT.decoder(messageBuffer);
        int batchSize = decoder.int32("batchSize");
        return new ServerStoreOpMessage.IteratorOpenMessage(batchSize);
      }
      case ITERATOR_CLOSE: {
        StructDecoder<Void> decoder = ITERATOR_CLOSE_STRUCT.decoder(messageBuffer);
        UUID identity = UUID.fromString(decoder.string("id"));
        return new ServerStoreOpMessage.IteratorCloseMessage(identity);
      }
      case ITERATOR_ADVANCE: {
        StructDecoder<Void> decoder = ITERATOR_ADVANCE_STRUCT.decoder(messageBuffer);
        UUID identity = UUID.fromString(decoder.string("id"));
        int batchSize = decoder.int32("batchSize");
        return new ServerStoreOpMessage.IteratorAdvanceMessage(identity, batchSize);
      }
      default:
        throw new RuntimeException("Unhandled message operation : " + opCode);
    }
  }

}
