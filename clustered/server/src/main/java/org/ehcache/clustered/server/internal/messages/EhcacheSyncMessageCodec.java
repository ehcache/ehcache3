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

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.ChainCodec;
import org.ehcache.clustered.common.internal.messages.ConfigCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.store.Chain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.SyncMessageCodec;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.nio.ByteBuffer.wrap;
import static org.ehcache.clustered.common.internal.messages.ChainCodec.CHAIN_STRUCT;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.KEY_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.SERVER_STORE_NAME_FIELD;
import static org.ehcache.clustered.common.internal.store.Util.marshall;
import static org.ehcache.clustered.common.internal.store.Util.unmarshall;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.DATA;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.STATE;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.STATE_REPO;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.SYNC_MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.SYNC_MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.SYNC_MESSAGE_TYPE_MAPPING;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class EhcacheSyncMessageCodec implements SyncMessageCodec<EhcacheEntityMessage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheSyncMessageCodec.class);

  private static final String STORES_SUB_STRUCT = "stores";
  private static final String CHAIN_FIELD = "chain";
  private static final String CHAIN_MAP_ENTRIES_SUB_STRUCT = "entries";
  private static final String STATE_REPO_ENTRIES_SUB_STRUCT = "mappings";
  private static final String STATE_REPO_VALUE_FIELD = "value";
  private static final String STATE_REPO_MAP_NAME_FIELD = "mapName";

  private static final Struct CHAIN_MAP_ENTRY_STRUCT = newStructBuilder()
    .int64(KEY_FIELD, 10)
    .struct(CHAIN_FIELD, 20, CHAIN_STRUCT)
    .build();

  private static final Struct DATA_SYNC_STRUCT = newStructBuilder()
    .enm(SYNC_MESSAGE_TYPE_FIELD_NAME, SYNC_MESSAGE_TYPE_FIELD_INDEX, SYNC_MESSAGE_TYPE_MAPPING)
    .string(SERVER_STORE_NAME_FIELD, 20)
    .structs(CHAIN_MAP_ENTRIES_SUB_STRUCT, 40, CHAIN_MAP_ENTRY_STRUCT)
    .build();

  private static final Struct STATE_REPO_ENTRY_STRUCT = newStructBuilder()
    .byteBuffer(KEY_FIELD, 10)
    .byteBuffer(STATE_REPO_VALUE_FIELD, 20)
    .build();

  private static final Struct STATE_REPO_SYNC_STRUCT = newStructBuilder()
    .enm(SYNC_MESSAGE_TYPE_FIELD_NAME, SYNC_MESSAGE_TYPE_FIELD_INDEX, SYNC_MESSAGE_TYPE_MAPPING)
    .string(SERVER_STORE_NAME_FIELD, 20)
    .string(STATE_REPO_MAP_NAME_FIELD, 30)
    .structs(STATE_REPO_ENTRIES_SUB_STRUCT, 40, STATE_REPO_ENTRY_STRUCT)
    .build();

  private final Struct stateSyncStruct;

  private final ChainCodec chainCodec;
  private final ConfigCodec configCodec;

  public EhcacheSyncMessageCodec(ConfigCodec configCodec) {
    this.configCodec = configCodec;
    this.chainCodec = new ChainCodec();

    StructBuilder stateSyncStructBuilder = newStructBuilder()
      .enm(SYNC_MESSAGE_TYPE_FIELD_NAME, SYNC_MESSAGE_TYPE_FIELD_INDEX, SYNC_MESSAGE_TYPE_MAPPING);
    ConfigCodec.InjectTuple tuple = configCodec.injectServerSideConfiguration(stateSyncStructBuilder, SYNC_MESSAGE_TYPE_FIELD_INDEX + 10);
    stateSyncStructBuilder = tuple.getUpdatedBuilder();
    int lastIndex = tuple.getLastIndex();

    StructBuilder storeConfigStructBuilder = newStructBuilder().string(SERVER_STORE_NAME_FIELD, 10);
    Struct storeConfigStruct = configCodec.injectServerStoreConfiguration(storeConfigStructBuilder, 20).getUpdatedBuilder().build();

    stateSyncStruct = stateSyncStructBuilder.structs(STORES_SUB_STRUCT, lastIndex + 10, storeConfigStruct).build();
  }

  @Override
  public byte[] encode(final int concurrencyKey, final EhcacheEntityMessage message) throws MessageCodecException {
    if (message instanceof EhcacheSyncMessage) {
      EhcacheSyncMessage syncMessage = (EhcacheSyncMessage) message;
      StructEncoder<Void> encoder;
      switch (syncMessage.getMessageType()) {
        case STATE: {
          encoder = stateSyncStruct.encoder();
          EhcacheStateSyncMessage stateSyncMessage = (EhcacheStateSyncMessage)syncMessage;
          encoder.enm(SYNC_MESSAGE_TYPE_FIELD_NAME, STATE);
          configCodec.encodeServerSideConfiguration(encoder, stateSyncMessage.getConfiguration());
          encoder.structs(STORES_SUB_STRUCT, stateSyncMessage.getStoreConfigs().entrySet(), (storeEncoder, storeEntry) -> {
            storeEncoder.string(SERVER_STORE_NAME_FIELD, storeEntry.getKey());
            configCodec.encodeServerStoreConfiguration(storeEncoder, storeEntry.getValue());
          });
          return encoder.encode().array();
        }
        case DATA: {
          encoder = DATA_SYNC_STRUCT.encoder();
          EhcacheDataSyncMessage dataSyncMessage = (EhcacheDataSyncMessage)syncMessage;
          encoder.enm(SYNC_MESSAGE_TYPE_FIELD_NAME, DATA);
          encoder.string(SERVER_STORE_NAME_FIELD, dataSyncMessage.getCacheId());
          encoder.structs(CHAIN_MAP_ENTRIES_SUB_STRUCT,
            dataSyncMessage.getChainMap().entrySet(), (entryEncoder, entry) -> {
              entryEncoder.int64(KEY_FIELD, entry.getKey());
              chainCodec.encode(entryEncoder.struct(CHAIN_FIELD), entry.getValue());
            });
          return encoder.encode().array();
        }
        case STATE_REPO: {
          encoder = STATE_REPO_SYNC_STRUCT.encoder();
          EhcacheStateRepoSyncMessage stateRepoSyncMessage = (EhcacheStateRepoSyncMessage) syncMessage;
          encoder.enm(SYNC_MESSAGE_TYPE_FIELD_NAME, STATE_REPO)
            .string(SERVER_STORE_NAME_FIELD, stateRepoSyncMessage.getCacheId())
            .string(STATE_REPO_MAP_NAME_FIELD, stateRepoSyncMessage.getMapId());
          encoder.structs(STATE_REPO_ENTRIES_SUB_STRUCT, stateRepoSyncMessage.getMappings().entrySet(),
            (entryEncoder, entry) -> entryEncoder.byteBuffer(KEY_FIELD, wrap(marshall(entry.getKey())))
              .byteBuffer(STATE_REPO_VALUE_FIELD, wrap(marshall(entry.getValue()))));
          return encoder.encode().array();
        }
        default:
          throw new IllegalArgumentException("Sync message codec can not encode " + syncMessage.getMessageType());
      }
    } else {
      throw new IllegalArgumentException(this.getClass().getName() + " can not encode " + message + " which is not a " + EhcacheStateSyncMessage.class);
    }
  }

  @Override
  public EhcacheSyncMessage decode(final int concurrencyKey, final byte[] payload) throws MessageCodecException {
    ByteBuffer message = wrap(payload);
    StructDecoder<Void> decoder = stateSyncStruct.decoder(message);
    Enm<SyncMessageType> enm = decoder.enm(SYNC_MESSAGE_TYPE_FIELD_NAME);
    if (!enm.isFound()) {
      throw new AssertionError("Invalid message format - misses the message type field");
    }
    if (!enm.isValid()) {
      LOGGER.warn("Unknown sync message received - ignoring {}", enm.raw());
      return null;
    }

    switch (enm.get()) {
      case STATE:
        ServerSideConfiguration configuration = configCodec.decodeServerSideConfiguration(decoder);
        Map<String, ServerStoreConfiguration> storeConfigs = decodeStoreConfigurations(decoder);
        return new EhcacheStateSyncMessage(configuration, storeConfigs);
      case DATA:
        message.rewind();
        decoder = DATA_SYNC_STRUCT.decoder(message);
        String storeName = decoder.string(SERVER_STORE_NAME_FIELD);
        Map<Long, Chain> chainMap = decodeChainMapEntries(decoder);
        return new EhcacheDataSyncMessage(storeName, chainMap);
      case STATE_REPO:
        message.rewind();
        decoder = STATE_REPO_SYNC_STRUCT.decoder(message);
        String storeId = decoder.string(SERVER_STORE_NAME_FIELD);
        String mapId = decoder.string(STATE_REPO_MAP_NAME_FIELD);
        ConcurrentMap<Object, Object> mappings = new ConcurrentHashMap<>();
        StructArrayDecoder<StructDecoder<Void>> structsDecoder = decoder.structs(STATE_REPO_ENTRIES_SUB_STRUCT);
        if (structsDecoder != null) {
          for (int i = 0; i < structsDecoder.length(); i++) {
            Object key = unmarshall(structsDecoder.byteBuffer(KEY_FIELD));
            Object value = unmarshall(structsDecoder.byteBuffer(STATE_REPO_VALUE_FIELD));
            mappings.put(key, value);
            structsDecoder.next();
          }
        }
        return new EhcacheStateRepoSyncMessage(storeId, mapId, mappings);
      default:
        throw new AssertionError("Cannot happen given earlier checks");
    }
  }

  private Map<Long, Chain> decodeChainMapEntries(StructDecoder<Void> decoder) {
    Map<Long, Chain> chainMap = new HashMap<>();

    StructArrayDecoder<? extends StructDecoder<?>> entriesDecoder = decoder.structs(CHAIN_MAP_ENTRIES_SUB_STRUCT);
    if (entriesDecoder != null) {
      for (int i = 0; i < entriesDecoder.length(); i++) {
        Long key = entriesDecoder.int64(KEY_FIELD);
        StructDecoder<? extends StructArrayDecoder<? extends StructDecoder<?>>> chainDecoder = entriesDecoder.struct(CHAIN_FIELD);
        Chain chain = chainCodec.decode(chainDecoder);
        chainMap.put(key, chain);
        entriesDecoder.next();
      }
    }
    return chainMap;
  }

  private Map<String, ServerStoreConfiguration> decodeStoreConfigurations(StructDecoder<Void> decoder) {
    Map<String, ServerStoreConfiguration> result = new HashMap<>();

    StructArrayDecoder<StructDecoder<Void>> storesDecoder = decoder.structs(STORES_SUB_STRUCT);
    if (storesDecoder != null) {
      for (int i = 0; i < storesDecoder.length(); i++) {
        String storeName = storesDecoder.string(SERVER_STORE_NAME_FIELD);
        result.put(storeName, configCodec.decodeServerStoreConfiguration(storesDecoder));
        storesDecoder.next();
      }
    }
    return result;
  }
}
