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
import org.junit.Test;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
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
}
