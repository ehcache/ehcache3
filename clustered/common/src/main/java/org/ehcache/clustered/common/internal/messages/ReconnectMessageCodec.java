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

import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.ArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.ArrayEncoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.util.HashSet;
import java.util.Set;

import static java.nio.ByteBuffer.wrap;
import static org.terracotta.runnel.StructBuilder.newStructBuilder;

public class ReconnectMessageCodec {

  private static final String HASH_INVALIDATION_IN_PROGRESS_FIELD = "hashInvalidationInProgress";
  private static final String CLEAR_IN_PROGRESS_FIELD = "clearInProgress";
  private static final String LOCKS_HELD_FIELD = "locksHeld";

  private static final Struct CLUSTER_TIER_RECONNECT_MESSAGE_STRUCT = newStructBuilder()
    .int64s(HASH_INVALIDATION_IN_PROGRESS_FIELD, 20)
    .bool(CLEAR_IN_PROGRESS_FIELD, 30)
    .int64s(LOCKS_HELD_FIELD, 40)
    .build();

  public byte[] encode(ClusterTierReconnectMessage reconnectMessage) {
    StructEncoder<Void> encoder = CLUSTER_TIER_RECONNECT_MESSAGE_STRUCT.encoder();
    ArrayEncoder<Long, StructEncoder<Void>> arrayEncoder = encoder.int64s(HASH_INVALIDATION_IN_PROGRESS_FIELD);
    reconnectMessage.getInvalidationsInProgress().forEach(arrayEncoder::value);
    encoder.bool(CLEAR_IN_PROGRESS_FIELD, reconnectMessage.isClearInProgress());
    ArrayEncoder<Long, StructEncoder<Void>> locksHeldEncoder = encoder.int64s(LOCKS_HELD_FIELD);
    reconnectMessage.getLocksHeld().forEach(locksHeldEncoder::value);
    return encoder.encode().array();
  }

  public ClusterTierReconnectMessage decode(byte[] payload) {
    StructDecoder<Void> decoder = CLUSTER_TIER_RECONNECT_MESSAGE_STRUCT.decoder(wrap(payload));
    ArrayDecoder<Long, StructDecoder<Void>> arrayDecoder = decoder.int64s(HASH_INVALIDATION_IN_PROGRESS_FIELD);

    Set<Long> hashes = decodeLongs(arrayDecoder);

    Boolean clearInProgress = decoder.bool(CLEAR_IN_PROGRESS_FIELD);

    ArrayDecoder<Long, StructDecoder<Void>> locksHeldDecoder = decoder.int64s(LOCKS_HELD_FIELD);
    Set<Long> locks = decodeLongs(locksHeldDecoder);

    ClusterTierReconnectMessage message = new ClusterTierReconnectMessage(hashes, locks, clearInProgress != null ? clearInProgress : false);



    return message;
  }

  private static Set<Long> decodeLongs(ArrayDecoder<Long, StructDecoder<Void>> decoder) {
    Set<Long> longs;
    if (decoder != null) {
      longs = new HashSet<>(decoder.length());
      for (int i = 0; i < decoder.length(); i++) {
        longs.add(decoder.value());
      }
    } else {
      longs = new HashSet<>(0);
    }
    return longs;
  }
}
