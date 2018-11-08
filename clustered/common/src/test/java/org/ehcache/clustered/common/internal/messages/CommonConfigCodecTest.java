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
import org.junit.Test;
import org.mockito.Mockito;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.SERVER_STORE_NAME_FIELD;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.terracotta.runnel.EnumMappingBuilder.newEnumMappingBuilder;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class CommonConfigCodecTest {

  private static final CommonConfigCodec CODEC = new CommonConfigCodec();

  @Test
  public void testEncodeDecodeServerSideConfiguration() throws Exception {
    ServerSideConfiguration serverSideConfiguration =
      new ServerSideConfiguration("foo", Collections.singletonMap("bar", new ServerSideConfiguration.Pool(1)));
    Struct serverSideConfigurationStruct = CODEC.injectServerSideConfiguration(newStructBuilder(), 10).getUpdatedBuilder().build();
    StructEncoder<Void> encoder = serverSideConfigurationStruct.encoder();
    CODEC.encodeServerSideConfiguration(encoder, serverSideConfiguration);
    ByteBuffer byteBuffer = encoder.encode();
    byteBuffer.rewind();
    ServerSideConfiguration decodedServerSideConfiguration =
      CODEC.decodeServerSideConfiguration(serverSideConfigurationStruct.decoder(byteBuffer));
    assertThat(decodedServerSideConfiguration.getDefaultServerResource(), is("foo"));
    assertThat(decodedServerSideConfiguration.getResourcePools(), hasKey("bar"));
  }

  @Test
  public void testInjectServerStoreConfiguration() {
    PoolAllocation poolAllocation = mock(PoolAllocation.class);
    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(poolAllocation, "Long.class",
            "String.class", null, null, Consistency.EVENTUAL, false, false);
    ConfigCodec.InjectTuple injectTuple = CODEC.injectServerStoreConfiguration(newStructBuilder(), 10);

    assertThat(injectTuple.getLastIndex(), is(40));

    Struct struct = injectTuple.getUpdatedBuilder().build();
    StructEncoder<Void> encoder = struct.encoder();

    CODEC.encodeServerStoreConfiguration(encoder, serverStoreConfiguration);

    encoder.int64(CommonConfigCodec.POOL_SIZE_FIELD, 20);

  }

  @Test
  public void testDecodeNonLoaderWriterServerStoreConfiguration() {
    EnumMapping<Consistency> consistencyEnumMapping = newEnumMappingBuilder(Consistency.class)
            .mapping(Consistency.EVENTUAL, 1)
            .mapping(Consistency.STRONG, 2)
            .build();
    int index = 30;
    StructBuilder builder = newStructBuilder()
        .string("identifier", 10)
        .string(SERVER_STORE_NAME_FIELD, 20)
        .string("keyType", index)
        .string("keySerializerType", index + 10)
        .string("valueType", index + 11)
        .string("valueSerializerType", index + 15)
        .enm("consistency", index + 16, consistencyEnumMapping)
        .int64("poolSize", index + 20)
        .string("resourceName", index + 30);

    Struct struct = builder.build();

    ByteBuffer encodedStoreConfig = struct.encoder()
        .string("identifier", "test")
        .string(SERVER_STORE_NAME_FIELD, "testStore")
        .string("keyType", "Long")
        .string("keySerializerType", "Long")
        .string("valueType", "Long")
        .string("valueSerializerType", "Long")
        .enm("consistency", Consistency.STRONG)
        .int64("poolSize", 20)
        .string("resourceName", "primary").encode();

    Struct newStruct = CODEC.injectServerStoreConfiguration(newStructBuilder()
            .string("identifier", 10)
            .string(SERVER_STORE_NAME_FIELD, 20), index)
            .getUpdatedBuilder()
            .build();
    encodedStoreConfig.flip();
    ServerStoreConfiguration serverStoreConfiguration =
            CODEC.decodeServerStoreConfiguration(newStruct.decoder(encodedStoreConfig));

    assertThat(serverStoreConfiguration.isLoaderWriterConfigured(), is(false));
    assertThat(serverStoreConfiguration.isWriteBehindConfigured(), is(false));

  }

}
