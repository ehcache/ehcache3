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

import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ChainReplicationMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ClearInvalidationCompleteMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ClientIDTrackerMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.InvalidationCompleteMessage;
import org.junit.Test;

import java.util.UUID;

import static java.nio.ByteBuffer.wrap;
import static org.ehcache.clustered.common.internal.store.Util.chainsEqual;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class PassiveReplicationMessageCodecTest {

  private PassiveReplicationMessageCodec codec = new PassiveReplicationMessageCodec();

  @Test
  public void testClientIDTrackerMessageCodec() {
    ClientIDTrackerMessage clientIDTrackerMessage = new ClientIDTrackerMessage(UUID.randomUUID());

    byte[] encoded = codec.encode(clientIDTrackerMessage);
    PassiveReplicationMessage decodedMsg = (PassiveReplicationMessage) codec.decode(EhcacheMessageType.CLIENT_ID_TRACK_OP, wrap(encoded));

    assertThat(decodedMsg.getClientId(), is(clientIDTrackerMessage.getClientId()));

  }

  @Test
  public void testChainReplicationMessageCodec() {
    Chain chain = getChain(false, createPayload(2L), createPayload(20L));
    ChainReplicationMessage chainReplicationMessage = new ChainReplicationMessage(2L, chain, 200L, UUID.randomUUID());

    byte[] encoded = codec.encode(chainReplicationMessage);
    ChainReplicationMessage decodedMsg = (ChainReplicationMessage) codec.decode(EhcacheMessageType.CHAIN_REPLICATION_OP, wrap(encoded));

    assertThat(decodedMsg.getClientId(), is(chainReplicationMessage.getClientId()));
    assertThat(decodedMsg.getId(), is(chainReplicationMessage.getId()));
    assertThat(decodedMsg.getKey(), is(chainReplicationMessage.getKey()));
    assertTrue(chainsEqual(decodedMsg.getChain(), chainReplicationMessage.getChain()));

  }

  @Test
  public void testClearInvalidationCompleteMessage() {
    ClearInvalidationCompleteMessage clearInvalidationCompleteMessage = new ClearInvalidationCompleteMessage();

    byte[] encoded = codec.encode(clearInvalidationCompleteMessage);
    ClearInvalidationCompleteMessage decoded = (ClearInvalidationCompleteMessage) codec.decode(EhcacheMessageType.CLEAR_INVALIDATION_COMPLETE, wrap(encoded));

    assertThat(decoded.getMessageType(), is(EhcacheMessageType.CLEAR_INVALIDATION_COMPLETE));

  }

  @Test
  public void testInvalidationCompleteMessage() {

    InvalidationCompleteMessage invalidationCompleteMessage = new InvalidationCompleteMessage(20L);

    byte[] encoded = codec.encode(invalidationCompleteMessage);
    InvalidationCompleteMessage decoded = (InvalidationCompleteMessage) codec.decode(EhcacheMessageType.INVALIDATION_COMPLETE, wrap(encoded));

    assertThat(decoded.getMessageType(), is(EhcacheMessageType.INVALIDATION_COMPLETE));
    assertThat(decoded.getKey(), equalTo(invalidationCompleteMessage.getKey()));
  }

}
