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
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.PrimitiveDecodingSupport;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.PrimitiveEncodingSupport;
import org.terracotta.runnel.encoding.StructEncoder;

/**
 *  Interface that allows extensions to codec.
 */
public interface ConfigCodec {
  InjectTuple injectServerSideConfiguration(StructBuilder baseBuilder, int index);
  void encodeServerSideConfiguration(StructEncoder<?> encoder, ServerSideConfiguration configuration);
  ServerSideConfiguration decodeServerSideConfiguration(StructDecoder<?> decoder);

  InjectTuple injectServerStoreConfiguration(StructBuilder baseBuilder, int index);
  void encodeServerStoreConfiguration(PrimitiveEncodingSupport<?> encoder, ServerStoreConfiguration configuration);
  ServerStoreConfiguration decodeServerStoreConfiguration(PrimitiveDecodingSupport decoder);

  interface InjectTuple {
    int getLastIndex();
    StructBuilder getUpdatedBuilder();
  }
}
