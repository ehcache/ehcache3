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
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.ResponseCodec;
import org.ehcache.clustered.common.internal.store.Chain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.SyncMessageCodec;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.nio.ByteBuffer.wrap;
import static org.ehcache.clustered.common.internal.messages.ChainCodec.CHAIN_STRUCT;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.KEY_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.SERVER_STORE_NAME_FIELD;
import static org.ehcache.clustered.common.internal.messages.StateRepositoryOpCodec.WHITELIST_PREDICATE;
import static org.ehcache.clustered.common.internal.store.Util.marshall;
import static org.ehcache.clustered.common.internal.store.Util.unmarshall;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.DATA;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.MESSAGE_TRACKER;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.STATE_REPO;
import static org.terracotta.runnel.EnumMappingBuilder.newEnumMappingBuilder;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class EhcacheSyncMessageCodec implements SyncMessageCodec<EhcacheEntityMessage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheSyncMessageCodec.class);

  private static final String SYNC_MESSAGE_TYPE_FIELD = "msgType";
  private static final String CHAIN_FIELD = "chain";
  private static final String CHAIN_MAP_ENTRIES_SUB_STRUCT = "entries";
  private static final String STATE_REPO_ENTRIES_SUB_STRUCT = "mappings";
  private static final String STATE_REPO_VALUE_FIELD = "value";
  private static final String STATE_REPO_MAP_NAME_FIELD = "mapName";
  private static final String MESSAGE_TRACKER_CLIENTS_STRUCT = "clients";
  private static final String MESSAGE_TRACKER_RESPONSES_STRUCT = "responses";
  private static final String MESSAGE_TRACKER_RESPONSE_FIELD = "response";
  private static final String MESSAGE_TRACKER_TRANSACTION_ID_FIELD = "tId";
  private static final String MESSAGE_TRACKER_SEGMENT_FIELD = "segment";

  private static final int SYNC_MESSAGE_TYPE_FIELD_INDEX = 10;
  private static final EnumMapping<SyncMessageType> SYNC_MESSAGE_TYPE_MAPPING = newEnumMappingBuilder(SyncMessageType.class)
    .mapping(STATE_REPO, 1)
    .mapping(DATA, 10)
    .mapping(MESSAGE_TRACKER, 20)
    .build();

  private static final Struct CHAIN_MAP_ENTRY_STRUCT = newStructBuilder()
    .int64(KEY_FIELD, 10)
    .struct(CHAIN_FIELD, 20, CHAIN_STRUCT)
    .build();

  private static final Struct DATA_SYNC_STRUCT = newStructBuilder()
    .enm(SYNC_MESSAGE_TYPE_FIELD, SYNC_MESSAGE_TYPE_FIELD_INDEX, SYNC_MESSAGE_TYPE_MAPPING)
    .structs(CHAIN_MAP_ENTRIES_SUB_STRUCT, 20, CHAIN_MAP_ENTRY_STRUCT)
    .build();

  private static final Struct STATE_REPO_ENTRY_STRUCT = newStructBuilder()
    .byteBuffer(KEY_FIELD, 10)
    .byteBuffer(STATE_REPO_VALUE_FIELD, 20)
    .build();

  private static final Struct STATE_REPO_SYNC_STRUCT = newStructBuilder()
    .enm(SYNC_MESSAGE_TYPE_FIELD, SYNC_MESSAGE_TYPE_FIELD_INDEX, SYNC_MESSAGE_TYPE_MAPPING)
    .string(SERVER_STORE_NAME_FIELD, 20)
    .string(STATE_REPO_MAP_NAME_FIELD, 30)
    .structs(STATE_REPO_ENTRIES_SUB_STRUCT, 40, STATE_REPO_ENTRY_STRUCT)
    .build();

  private static final Struct MESSAGE_TRACKER_RESPONSE_STRUCT = newStructBuilder()
    .int64(MESSAGE_TRACKER_TRANSACTION_ID_FIELD, 10)
    .byteBuffer(MESSAGE_TRACKER_RESPONSE_FIELD, 20)
    .build();

  private static final Struct MESSAGE_TRACKER_CLIENT_STRUCT = newStructBuilder()
    .int64(KEY_FIELD, 10)
    .structs(MESSAGE_TRACKER_RESPONSES_STRUCT, 20, MESSAGE_TRACKER_RESPONSE_STRUCT)
    .build();

  private static final Struct MESSAGE_TRACKER_SYNC_STRUCT = newStructBuilder()
    .enm(SYNC_MESSAGE_TYPE_FIELD, SYNC_MESSAGE_TYPE_FIELD_INDEX, SYNC_MESSAGE_TYPE_MAPPING)
    .structs(MESSAGE_TRACKER_CLIENTS_STRUCT, 20, MESSAGE_TRACKER_CLIENT_STRUCT)
    .int32(MESSAGE_TRACKER_SEGMENT_FIELD, 30)
    .build();

  private final ResponseCodec responseCodec;

  public EhcacheSyncMessageCodec(ResponseCodec responseCodec) {
    this.responseCodec = responseCodec;
  }

  @Override
  public byte[] encode(final int concurrencyKey, final EhcacheEntityMessage message) {
    if (message instanceof EhcacheSyncMessage) {
      EhcacheSyncMessage syncMessage = (EhcacheSyncMessage) message;
      switch (syncMessage.getMessageType()) {
        case DATA:
          return encodeDataSync((EhcacheDataSyncMessage) syncMessage);
        case STATE_REPO:
          return encodeStateRepoSync((EhcacheStateRepoSyncMessage) syncMessage);
        case MESSAGE_TRACKER:
          return encodeMessageTrackerSync((EhcacheMessageTrackerMessage) syncMessage);
        default:
          throw new IllegalArgumentException("Sync message codec can not encode " + syncMessage.getMessageType());
      }
    } else {
      throw new IllegalArgumentException(this.getClass().getName() + " can not encode " + message + " which is not a " + EhcacheSyncMessage.class.getName());
    }
  }

  private byte[] encodeMessageTrackerSync(EhcacheMessageTrackerMessage syncMessage) {
    StructEncoder<Void> encoder = MESSAGE_TRACKER_SYNC_STRUCT.encoder();
    encoder
      .enm(SYNC_MESSAGE_TYPE_FIELD, MESSAGE_TRACKER)
      .structs(MESSAGE_TRACKER_CLIENTS_STRUCT, syncMessage.getTrackedMessages().entrySet(),
        (clientEncoder, entry) -> {
          Map<Long, EhcacheEntityResponse> responses = entry.getValue();
          // Skip clients without tracked messages. It is useless to send them
          if(!responses.isEmpty()) {
            clientEncoder.int64(KEY_FIELD, entry.getKey());
            clientEncoder.structs(MESSAGE_TRACKER_RESPONSES_STRUCT, responses.entrySet(),
              (responseEncoder, response) -> {
                responseEncoder.int64(MESSAGE_TRACKER_TRANSACTION_ID_FIELD, response.getKey());
                responseEncoder.byteBuffer(MESSAGE_TRACKER_RESPONSE_FIELD, encodeResponse(response.getValue()));
              });
          }
        });

    return encoder.encode().array();
  }

  private ByteBuffer encodeResponse(EhcacheEntityResponse response) {
    return ByteBuffer.wrap(responseCodec.encode(response));
  }

  private byte[] encodeStateRepoSync(EhcacheStateRepoSyncMessage syncMessage) {
    StructEncoder<Void> encoder = STATE_REPO_SYNC_STRUCT.encoder();
    encoder.enm(SYNC_MESSAGE_TYPE_FIELD, STATE_REPO)
      .string(SERVER_STORE_NAME_FIELD, syncMessage.getCacheId())
      .string(STATE_REPO_MAP_NAME_FIELD, syncMessage.getMapId());
    encoder.structs(STATE_REPO_ENTRIES_SUB_STRUCT, syncMessage.getMappings().entrySet(),
      (entryEncoder, entry) -> entryEncoder.byteBuffer(KEY_FIELD, wrap(marshall(entry.getKey())))
        .byteBuffer(STATE_REPO_VALUE_FIELD, wrap(marshall(entry.getValue()))));
    return encoder.encode().array();
  }

  private byte[] encodeDataSync(EhcacheDataSyncMessage syncMessage) {
    StructEncoder<Void> encoder;
    encoder = DATA_SYNC_STRUCT.encoder();
    encoder.enm(SYNC_MESSAGE_TYPE_FIELD, DATA);
    encoder.structs(CHAIN_MAP_ENTRIES_SUB_STRUCT,
      syncMessage.getChainMap().entrySet(), (entryEncoder, entry) -> {
        entryEncoder.int64(KEY_FIELD, entry.getKey());
        entryEncoder.struct(CHAIN_FIELD, entry.getValue(), ChainCodec::encode);
      });
    return encoder.encode().array();
  }

  @Override
  public EhcacheSyncMessage decode(final int concurrencyKey, final byte[] payload) {
    ByteBuffer message = wrap(payload);
    StructDecoder<Void> decoder = DATA_SYNC_STRUCT.decoder(message);
    Enm<SyncMessageType> enm = decoder.enm(SYNC_MESSAGE_TYPE_FIELD);
    if (!enm.isFound()) {
      throw new AssertionError("Invalid message format - misses the message type field");
    }
    if (!enm.isValid()) {
      LOGGER.warn("Unknown sync message received - ignoring {}", enm.raw());
      return null;
    }

    // Now that we have the type, go back to the beginning of the message to re-read it into the actual message
    message.rewind();

    switch (enm.get()) {
      case DATA:
        return decodeDataSync(message);
      case STATE_REPO:
        return decodeStateRepoSync(message);
      case MESSAGE_TRACKER:
        return decodeMessageTracker(message);
      default:
        throw new AssertionError("Cannot happen given earlier checks");
    }
  }

  private EhcacheSyncMessage decodeMessageTracker(ByteBuffer message) {
    StructDecoder<Void> decoder = MESSAGE_TRACKER_SYNC_STRUCT.decoder(message);
    Map<Long, Map<Long, EhcacheEntityResponse>> trackedMessages = new HashMap<>();
    StructArrayDecoder<StructDecoder<Void>> clientsDecoder = decoder.structs(MESSAGE_TRACKER_CLIENTS_STRUCT);
    if(clientsDecoder != null) {
      while(clientsDecoder.hasNext()) {
        StructDecoder<StructArrayDecoder<StructDecoder<Void>>> clientDecoder = clientsDecoder.next();
        Long clientId = clientDecoder.int64(KEY_FIELD);
        HashMap<Long, EhcacheEntityResponse> responses = new HashMap<>();
        trackedMessages.put(clientId, responses);
        StructArrayDecoder<StructDecoder<StructArrayDecoder<StructDecoder<Void>>>> responsesDecoder = clientDecoder.structs(MESSAGE_TRACKER_RESPONSES_STRUCT);
        if(responsesDecoder != null) {
          while (responsesDecoder.hasNext()) {
            StructDecoder<StructArrayDecoder<StructDecoder<StructArrayDecoder<StructDecoder<Void>>>>> responseDecoder = responsesDecoder.next();
            Long transactionId = responseDecoder.int64(MESSAGE_TRACKER_TRANSACTION_ID_FIELD);
            ByteBuffer bb = responseDecoder.byteBuffer(MESSAGE_TRACKER_RESPONSE_FIELD);
            byte[] encodedResponse = new byte[bb.remaining()];
            bb.get(encodedResponse);
            EhcacheEntityResponse res = responseCodec.decode(encodedResponse);
            responses.put(transactionId, res);
          }
        }
      }
    }
    return new EhcacheMessageTrackerMessage(trackedMessages);
  }

  private EhcacheSyncMessage decodeStateRepoSync(ByteBuffer message) {
    StructDecoder<Void> decoder = STATE_REPO_SYNC_STRUCT.decoder(message);
    String storeId = decoder.string(SERVER_STORE_NAME_FIELD);
    String mapId = decoder.string(STATE_REPO_MAP_NAME_FIELD);
    ConcurrentMap<Object, Object> mappings = new ConcurrentHashMap<>();
    StructArrayDecoder<StructDecoder<Void>> structsDecoder = decoder.structs(STATE_REPO_ENTRIES_SUB_STRUCT);
    if (structsDecoder != null) {
      while (structsDecoder.hasNext()) {
        StructDecoder<StructArrayDecoder<StructDecoder<Void>>> structDecoder = structsDecoder.next();
        Object key = unmarshall(structDecoder.byteBuffer(KEY_FIELD), WHITELIST_PREDICATE);
        Object value = unmarshall(structDecoder.byteBuffer(STATE_REPO_VALUE_FIELD), WHITELIST_PREDICATE);
        mappings.put(key, value);
      }
    }
    return new EhcacheStateRepoSyncMessage(storeId, mapId, mappings);
  }

  private EhcacheSyncMessage decodeDataSync(ByteBuffer message) {
    StructDecoder<Void> decoder = DATA_SYNC_STRUCT.decoder(message);
    Map<Long, Chain> chainMap = decodeChainMapEntries(decoder);
    return new EhcacheDataSyncMessage(chainMap);
  }

  private Map<Long, Chain> decodeChainMapEntries(StructDecoder<Void> decoder) {
    StructArrayDecoder<? extends StructDecoder<?>> entriesDecoder = decoder.structs(CHAIN_MAP_ENTRIES_SUB_STRUCT);

    if (entriesDecoder != null) {
      int len = entriesDecoder.length();
      Map<Long, Chain> chainMap = new HashMap<>((int) ((float) len / 0.75f + 1.0f));
      for (int i = 0; i < len; i++) {
        StructDecoder<?> entryDecoder = entriesDecoder.next();
        Long key = entryDecoder.int64(KEY_FIELD);
        StructDecoder<?> chainDecoder = entryDecoder.struct(CHAIN_FIELD);
        Chain chain = ChainCodec.decode(chainDecoder);
        chainMap.put(key, chain);
        entryDecoder.end();
      }
      return chainMap;
    } else {
      return Collections.emptyMap();
    }
  }
}
