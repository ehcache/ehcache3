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
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.MessageCodecUtils;
import org.ehcache.clustered.common.internal.store.Chain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.SyncMessageCodec;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.ehcache.clustered.common.internal.messages.ChainCodec.CHAIN_STRUCT;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.CONSISTENCY_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.DEFAULT_RESOURCE_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.KEY_FIELD;
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
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.DATA;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.STATE;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.SYNC_MESSAGE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.SYNC_MESSAGE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.server.internal.messages.SyncMessageType.SYNC_MESSAGE_TYPE_MAPPING;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class EhcacheSyncMessageCodec implements SyncMessageCodec<EhcacheEntityMessage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheSyncMessageCodec.class);

  private static final String STORES_SUB_STRUCT = "stores";
  private static final String CHAIN_FIELD = "chain";

  private static final Struct POOLS_STRUCT = newStructBuilder()
    .string(POOL_NAME_FIELD, 10)
    .int64(POOL_SIZE_FIELD, 20)
    .string(POOL_RESOURCE_NAME_FIELD, 30).build();

  private static final Struct SERVER_STORE_CONFIGURATION_STRUCT = newStructBuilder()
    .string(SERVER_STORE_NAME_FIELD, 10)
    .string(STORE_CONFIG_KEY_TYPE_FIELD, 20)
    .string(STORE_CONFIG_KEY_SERIALIZER_TYPE_FIELD, 21)
    .string(STORE_CONFIG_VALUE_TYPE_FIELD, 25)
    .string(STORE_CONFIG_VALUE_SERIALIZER_TYPE_FIELD, 26)
    .enm(STORE_CONFIG_CONSISTENCY_FIELD, 30, CONSISTENCY_ENUM_MAPPING)
    .int64(POOL_SIZE_FIELD, 40)
    .string(POOL_RESOURCE_NAME_FIELD, 45)
    .build();

  private static final Struct STATE_SYNC_STRUCT = newStructBuilder()
    .enm(SYNC_MESSAGE_TYPE_FIELD_NAME, SYNC_MESSAGE_TYPE_FIELD_INDEX, SYNC_MESSAGE_TYPE_MAPPING)
    .string(DEFAULT_RESOURCE_FIELD, 20)
    .structs(POOLS_SUB_STRUCT, 30, POOLS_STRUCT)
    .structs(STORES_SUB_STRUCT, 40, SERVER_STORE_CONFIGURATION_STRUCT)
    .build();

  private static final Struct DATA_SYNC_STRUCT = newStructBuilder()
    .enm(SYNC_MESSAGE_TYPE_FIELD_NAME, SYNC_MESSAGE_TYPE_FIELD_INDEX, SYNC_MESSAGE_TYPE_MAPPING)
    .string(SERVER_STORE_NAME_FIELD, 20)
    .int64(KEY_FIELD, 30)
    .struct(CHAIN_FIELD, 40, CHAIN_STRUCT)
    .build();

  private final ChainCodec chainCodec = new ChainCodec();
  private final MessageCodecUtils codecUtils = new MessageCodecUtils();

  @Override
  public byte[] encode(final int concurrencyKey, final EhcacheEntityMessage message) throws MessageCodecException {
    if (message instanceof EhcacheSyncMessage) {
      EhcacheSyncMessage syncMessage = (EhcacheSyncMessage) message;
      StructEncoder encoder;
      switch (syncMessage.getMessageType()) {
        case STATE:
          encoder = STATE_SYNC_STRUCT.encoder();
          EhcacheStateSyncMessage stateSyncMessage = (EhcacheStateSyncMessage) syncMessage;
          encoder.enm(SYNC_MESSAGE_TYPE_FIELD_NAME, STATE);
          encodeServerSideConfiguration(encoder, stateSyncMessage.getConfiguration());
          encoder.structs(STORES_SUB_STRUCT, stateSyncMessage.getStoreConfigs().entrySet(), (storeEncoder, storeEntry) -> {
            storeEncoder.string(SERVER_STORE_NAME_FIELD, storeEntry.getKey());
            codecUtils.encodeServerStoreConfiguration(storeEncoder, storeEntry.getValue());
          });
          return encoder.encode().array();
        case DATA:
          encoder = DATA_SYNC_STRUCT.encoder();
          EhcacheDataSyncMessage dataSyncMessage = (EhcacheDataSyncMessage) syncMessage;
          encoder.enm(SYNC_MESSAGE_TYPE_FIELD_NAME, DATA);
          encoder.string(SERVER_STORE_NAME_FIELD, dataSyncMessage.getCacheId());
          encoder.int64(KEY_FIELD, dataSyncMessage.getKey());
          encoder.struct(CHAIN_FIELD, (chainEncoder) -> chainCodec.encode(chainEncoder, dataSyncMessage.getChain()));
          return encoder.encode().array();
        default:
          throw new IllegalArgumentException("Sync message codec can not encode " + syncMessage.getMessageType());
      }
    } else {
      throw new IllegalArgumentException(this.getClass().getName() + " can not encode " + message + " which is not a " + EhcacheStateSyncMessage.class);
    }
  }

  private void encodeServerSideConfiguration(StructEncoder encoder, ServerSideConfiguration configuration) {
    if (configuration.getDefaultServerResource() != null) {
      encoder.string(DEFAULT_RESOURCE_FIELD, configuration.getDefaultServerResource());
    }
    encoder.structs(POOLS_SUB_STRUCT, configuration.getResourcePools().entrySet(), (poolEncoder, poolEntry) -> {
      poolEncoder.string(POOL_NAME_FIELD, poolEntry.getKey());
      ServerSideConfiguration.Pool pool = poolEntry.getValue();
      poolEncoder.int64(POOL_SIZE_FIELD, pool.getSize());
      if (pool.getServerResource() != null) {
        poolEncoder.string(POOL_RESOURCE_NAME_FIELD, pool.getServerResource());
      }
    });
  }

  @Override
  public EhcacheSyncMessage decode(final int concurrencyKey, final byte[] payload) throws MessageCodecException {
    ByteBuffer message = ByteBuffer.wrap(payload);
    StructDecoder decoder = STATE_SYNC_STRUCT.decoder(message);
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
        ServerSideConfiguration configuration = decodeServerSideConfiguration(decoder);
        Map<String, ServerStoreConfiguration> storeConfigs = decodeStoreConfigurations(decoder);
        return new EhcacheStateSyncMessage(configuration, storeConfigs);
      case DATA:
        message.rewind();
        decoder = DATA_SYNC_STRUCT.decoder(message);
        String storeName = decoder.string(SERVER_STORE_NAME_FIELD);
        Long key = decoder.int64(KEY_FIELD);
        Chain chain = chainCodec.decode(decoder.struct(CHAIN_FIELD));
        return new EhcacheDataSyncMessage(storeName, key, chain);
      default:
        throw new AssertionError("Cannot happen given earlier checks");
    }
  }

  private Map<String, ServerStoreConfiguration> decodeStoreConfigurations(StructDecoder decoder) {
    Map<String, ServerStoreConfiguration> result = new HashMap<>();

    StructArrayDecoder storesDecoder = decoder.structs(STORES_SUB_STRUCT);
    if (storesDecoder != null) {
      for (int i = 0; i < storesDecoder.length(); i++) {
        String storeName = storesDecoder.string(SERVER_STORE_NAME_FIELD);
        result.put(storeName, codecUtils.decodeServerStoreConfiguration(storesDecoder));
        storesDecoder.next();
      }
    }
    return result;
  }

  private ServerSideConfiguration decodeServerSideConfiguration(StructDecoder decoder) {
    String defaultResource = decoder.string(DEFAULT_RESOURCE_FIELD);
    Map<String, ServerSideConfiguration.Pool> pools = new HashMap<>();
    StructArrayDecoder poolsDecoder = decoder.structs(POOLS_SUB_STRUCT);
    if (poolsDecoder != null) {
      for (int i = 0; i < poolsDecoder.length(); i++) {
        String poolName = poolsDecoder.string(POOL_NAME_FIELD);
        Long poolSize = poolsDecoder.int64(POOL_SIZE_FIELD);
        String poolResource = poolsDecoder.string(POOL_RESOURCE_NAME_FIELD);
        pools.put(poolName, new ServerSideConfiguration.Pool(poolSize, poolResource));
        poolsDecoder.next();
      }
    }
    if (defaultResource == null) {
      return new ServerSideConfiguration(pools);
    } else {
      return new ServerSideConfiguration(defaultResource, pools);
    }
  }
}
