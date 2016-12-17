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
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.PrimitiveDecodingSupport;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.PrimitiveEncodingSupport;
import org.terracotta.runnel.encoding.StructArrayEncoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.util.HashMap;
import java.util.Map;

import static org.terracotta.runnel.EnumMappingBuilder.newEnumMappingBuilder;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

/**
 * Encodes and decodes configuration objects such as {@link ServerSideConfiguration} and {@link ServerStoreConfiguration}.
 * <p>
 * This class is made extensible and hence must remain public.
 */
@SuppressWarnings("WeakerAccess")
public class CommonConfigCodec implements ConfigCodec {

  private static final String STORE_CONFIG_KEY_TYPE_FIELD = "keyType";
  private static final String STORE_CONFIG_KEY_SERIALIZER_TYPE_FIELD = "keySerializerType";
  private static final String STORE_CONFIG_VALUE_TYPE_FIELD = "valueType";
  private static final String STORE_CONFIG_VALUE_SERIALIZER_TYPE_FIELD = "valueSerializerType";
  private static final String STORE_CONFIG_CONSISTENCY_FIELD = "consistency";
  private static final String POOL_SIZE_FIELD = "poolSize";
  private static final String POOL_RESOURCE_NAME_FIELD = "resourceName";
  private static final String DEFAULT_RESOURCE_FIELD = "defaultResource";
  private static final String POOLS_SUB_STRUCT = "pools";
  private static final String POOL_NAME_FIELD = "poolName";

  private static final EnumMapping<Consistency> CONSISTENCY_ENUM_MAPPING = newEnumMappingBuilder(Consistency.class)
    .mapping(Consistency.EVENTUAL, 1)
    .mapping(Consistency.STRONG, 2)
    .build();

  private static final Struct POOLS_STRUCT = newStructBuilder()
    .string(POOL_NAME_FIELD, 10)
    .int64(POOL_SIZE_FIELD, 20)
    .string(POOL_RESOURCE_NAME_FIELD, 30).build();

  @Override
  public InjectTuple injectServerStoreConfiguration(StructBuilder baseBuilder, final int index) {
    final StructBuilder structBuilder = baseBuilder.string(STORE_CONFIG_KEY_TYPE_FIELD, index)
      .string(STORE_CONFIG_KEY_SERIALIZER_TYPE_FIELD, index + 10)
      .string(STORE_CONFIG_VALUE_TYPE_FIELD, index + 11)
      .string(STORE_CONFIG_VALUE_SERIALIZER_TYPE_FIELD, index + 15)
      .enm(STORE_CONFIG_CONSISTENCY_FIELD, index + 16, CONSISTENCY_ENUM_MAPPING)
      .int64(POOL_SIZE_FIELD, index + 20)
      .string(POOL_RESOURCE_NAME_FIELD, index + 30);

    return new InjectTuple() {
      @Override
      public int getLastIndex() {
        return index + 30;
      }

      @Override
      public StructBuilder getUpdatedBuilder() {
        return structBuilder;
      }
    };
  }

  @Override
  public InjectTuple injectServerSideConfiguration(StructBuilder baseBuilder, final int index) {
    final StructBuilder structBuilder = baseBuilder.string(DEFAULT_RESOURCE_FIELD, index + 10)
      .structs(POOLS_SUB_STRUCT, index + 20, POOLS_STRUCT);

    return new InjectTuple() {
      @Override
      public int getLastIndex() {
        return index + 20;
      }

      @Override
      public StructBuilder getUpdatedBuilder() {
        return structBuilder;
      }
    };
  }

  @Override
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

  @Override
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

  @Override
  public void encodeServerSideConfiguration(StructEncoder<?> encoder, ServerSideConfiguration configuration) {
    if (configuration.getDefaultServerResource() != null) {
      encoder.string(DEFAULT_RESOURCE_FIELD, configuration.getDefaultServerResource());
    }

    if (!configuration.getResourcePools().isEmpty()) {
      StructArrayEncoder<? extends StructEncoder<?>> poolsEncoder = encoder.structs(POOLS_SUB_STRUCT);
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

  @Override
  public ServerSideConfiguration decodeServerSideConfiguration(StructDecoder<?> decoder) {
    String defaultResource = decoder.string(DEFAULT_RESOURCE_FIELD);

    HashMap<String, ServerSideConfiguration.Pool> resourcePools = new HashMap<String, ServerSideConfiguration.Pool>();
    StructArrayDecoder<? extends StructDecoder<?>> poolsDecoder = decoder.structs(POOLS_SUB_STRUCT);
    if (poolsDecoder != null) {
      for (int i = 0; i < poolsDecoder.length(); i++) {
        String poolName = poolsDecoder.string(POOL_NAME_FIELD);
        Long poolSize = poolsDecoder.int64(POOL_SIZE_FIELD);
        String poolResourceName = poolsDecoder.string(POOL_RESOURCE_NAME_FIELD);
        if (poolResourceName == null) {
          resourcePools.put(poolName, new ServerSideConfiguration.Pool(poolSize));
        } else {
          resourcePools.put(poolName, new ServerSideConfiguration.Pool(poolSize, poolResourceName));
        }
        poolsDecoder.next();
      }
    }

    ServerSideConfiguration serverSideConfiguration;
    if (defaultResource == null) {
      serverSideConfiguration = new ServerSideConfiguration(resourcePools);
    } else {
      serverSideConfiguration = new ServerSideConfiguration(defaultResource, resourcePools);
    }
    return serverSideConfiguration;
  }
}
