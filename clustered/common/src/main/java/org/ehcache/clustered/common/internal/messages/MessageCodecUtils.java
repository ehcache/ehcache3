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

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.PrimitiveDecodingSupport;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.PrimitiveEncodingSupport;
import org.terracotta.runnel.encoding.StructEncoder;

import java.util.UUID;

import static org.terracotta.runnel.EnumMappingBuilder.newEnumMappingBuilder;

/**
 * MessageCodecUtils
 */
public class MessageCodecUtils {

  public static final String MSG_ID_FIELD = "msgId";
  public static final String LSB_UUID_FIELD = "lsbUUID";
  public static final String MSB_UUID_FIELD = "msbUUID";
  public static final String SERVER_STORE_NAME_FIELD = "serverStoreName";
  public static final String KEY_FIELD = "key";
  public static final String DEFAULT_RESOURCE_FIELD = "defaultResource";
  public static final String STORE_CONFIG_KEY_TYPE_FIELD = "keyType";
  public static final String STORE_CONFIG_KEY_SERIALIZER_TYPE_FIELD = "keySerializerType";
  public static final String STORE_CONFIG_VALUE_TYPE_FIELD = "valueType";
  public static final String STORE_CONFIG_VALUE_SERIALIZER_TYPE_FIELD = "valueSerializerType";
  public static final String STORE_CONFIG_CONSISTENCY_FIELD = "consistency";
  public static final String POOLS_SUB_STRUCT = "pools";
  public static final String POOL_NAME_FIELD = "poolName";
  public static final String POOL_SIZE_FIELD = "poolSize";
  public static final String POOL_RESOURCE_NAME_FIELD = "resourceName";

  public static final EnumMapping<Consistency> CONSISTENCY_ENUM_MAPPING = newEnumMappingBuilder(Consistency.class)
    .mapping(Consistency.EVENTUAL, 1)
    .mapping(Consistency.STRONG, 2)
    .build();

  public void encodeMandatoryFields(StructEncoder encoder, EhcacheOperationMessage message) {
    encoder.enm(EhcacheMessageType.MESSAGE_TYPE_FIELD_NAME, message.getMessageType())
      .int64(MSG_ID_FIELD, message.getId())
      .int64(MSB_UUID_FIELD, message.getClientId().getMostSignificantBits())
      .int64(LSB_UUID_FIELD, message.getClientId().getLeastSignificantBits());
  }

  public UUID decodeUUID(StructDecoder decoder) {
    return new UUID(decoder.int64(MSB_UUID_FIELD), decoder.int64(LSB_UUID_FIELD));
  }

  public void encodeServerStoreConfiguration(PrimitiveEncodingSupport<?> encoder, ServerStoreConfiguration configuration) {
    encoder.string(STORE_CONFIG_KEY_TYPE_FIELD, configuration.getStoredKeyType())
      .string(STORE_CONFIG_KEY_SERIALIZER_TYPE_FIELD, configuration.getKeySerializerType())
      .string(STORE_CONFIG_VALUE_TYPE_FIELD, configuration.getStoredValueType())
      .string(STORE_CONFIG_VALUE_SERIALIZER_TYPE_FIELD, configuration.getValueSerializerType());
    if (configuration.getConsistency() != null) {
      encoder.enm(STORE_CONFIG_CONSISTENCY_FIELD, configuration.getConsistency());
    }

    PoolAllocation poolAllocation = configuration.getPoolAllocation();
    if (poolAllocation instanceof PoolAllocation.Dedicated) {
      PoolAllocation.Dedicated dedicatedPool = (PoolAllocation.Dedicated) poolAllocation;
      encoder.int64(POOL_SIZE_FIELD, dedicatedPool.getSize());
      if (dedicatedPool.getResourceName() != null) {
        encoder.string(POOL_RESOURCE_NAME_FIELD, dedicatedPool.getResourceName());
      }
    } else if (poolAllocation instanceof PoolAllocation.Shared) {
      encoder.string(POOL_RESOURCE_NAME_FIELD, ((PoolAllocation.Shared) poolAllocation).getResourcePoolName());
    }
  }

  public ServerStoreConfiguration decodeServerStoreConfiguration(PrimitiveDecodingSupport decoder) {
    String keyType = decoder.string(STORE_CONFIG_KEY_TYPE_FIELD);
    String keySerializer = decoder.string(STORE_CONFIG_KEY_SERIALIZER_TYPE_FIELD);
    String valueType = decoder.string(STORE_CONFIG_VALUE_TYPE_FIELD);
    String valueSerializer = decoder.string(STORE_CONFIG_VALUE_SERIALIZER_TYPE_FIELD);
    Enm<Consistency> consistencyEnm = decoder.enm(STORE_CONFIG_CONSISTENCY_FIELD);
    Consistency consistency = Consistency.EVENTUAL;
    if (consistencyEnm.isValid()) {
      consistency = consistencyEnm.get();
    }
    Long poolSize = decoder.int64(POOL_SIZE_FIELD);
    String poolResource = decoder.string(POOL_RESOURCE_NAME_FIELD);
    PoolAllocation poolAllocation = new PoolAllocation.Unknown();
    if (poolSize != null) {
      poolAllocation = new PoolAllocation.Dedicated(poolResource, poolSize);
    } else if (poolResource != null) {
      poolAllocation = new PoolAllocation.Shared(poolResource);
    }
    return new ServerStoreConfiguration(poolAllocation, keyType, valueType, null, null, keySerializer, valueSerializer, consistency);
  }
}
