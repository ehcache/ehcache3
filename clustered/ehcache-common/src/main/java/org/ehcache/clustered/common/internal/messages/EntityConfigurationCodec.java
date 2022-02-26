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
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.store.ClusterTierEntityConfiguration;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.wrap;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.SERVER_STORE_NAME_FIELD;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

/**
 * EntityConfigurationCodec
 */
public class EntityConfigurationCodec {

  private static final String IDENTIFIER = "identifier";

  private final ConfigCodec configCodec;
  private final Struct tierManagerConfigurationStruct;
  private final Struct clusteredStoreConfigurationStruct;

  public EntityConfigurationCodec(ConfigCodec configCodec) {
    this.configCodec = configCodec;

    StructBuilder tierManagerConfigurationBaseStruct = newStructBuilder()
      .string(IDENTIFIER, 10);

    tierManagerConfigurationStruct = configCodec.injectServerSideConfiguration(tierManagerConfigurationBaseStruct, 10)
      .getUpdatedBuilder()
      .build();

    StructBuilder clusteredStoreConfigurationBaseStruct = newStructBuilder()
      .string(IDENTIFIER, 10)
      .string(SERVER_STORE_NAME_FIELD, 20);

    clusteredStoreConfigurationStruct = configCodec.injectServerStoreConfiguration(clusteredStoreConfigurationBaseStruct, 30)
      .getUpdatedBuilder()
      .build();
  }

  public byte[] encode(ClusterTierEntityConfiguration configuration) {
    StructEncoder<Void> encoder = clusteredStoreConfigurationStruct.encoder();
    encoder.string(IDENTIFIER, configuration.getManagerIdentifier())
      .string(SERVER_STORE_NAME_FIELD, configuration.getStoreIdentifier());
    configCodec.encodeServerStoreConfiguration(encoder, configuration.getConfiguration());
    return encoder.encode().array();
  }

  public ClusterTierEntityConfiguration decodeClusteredStoreConfiguration(byte[] configuration) {
    return decodeClusteredStoreConfiguration(wrap(configuration));
  }

  public ClusterTierEntityConfiguration decodeClusteredStoreConfiguration(ByteBuffer buffer) {
    StructDecoder<Void> decoder = clusteredStoreConfigurationStruct.decoder(buffer);
    String managerIdentifier = decoder.string(IDENTIFIER);
    if (managerIdentifier == null) {
      throw new IllegalArgumentException("Payload is an invalid content");
    }
    String storeIdentifier = decoder.string(SERVER_STORE_NAME_FIELD);
    ServerStoreConfiguration serverStoreConfiguration = configCodec.decodeServerStoreConfiguration(decoder);
    return new ClusterTierEntityConfiguration(managerIdentifier, storeIdentifier, serverStoreConfiguration);
  }
  public byte[] encode(ClusterTierManagerConfiguration configuration) {
    StructEncoder<Void> encoder = tierManagerConfigurationStruct.encoder();
    encoder.string(IDENTIFIER, configuration.getIdentifier());
    configCodec.encodeServerSideConfiguration(encoder, configuration.getConfiguration());
    return encoder.encode().array();
  }

  public ClusterTierManagerConfiguration decodeClusterTierManagerConfiguration(byte[] payload) {
    StructDecoder<Void> decoder = tierManagerConfigurationStruct.decoder(wrap(payload));
    String identifier = decoder.string(IDENTIFIER);
    if (identifier == null) {
      throw new IllegalArgumentException("Payload is an invalid content");
    }
    ServerSideConfiguration configuration = configCodec.decodeServerSideConfiguration(decoder);
    if (configuration == null) {
      throw new AssertionError("Creation configuration cannot be null");
    }
    return new ClusterTierManagerConfiguration(identifier, configuration);
  }
}
