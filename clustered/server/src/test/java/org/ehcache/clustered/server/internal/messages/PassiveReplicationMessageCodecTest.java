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
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.InvalidationCompleteMessage;
import org.junit.Test;

import static java.nio.ByteBuffer.wrap;
import static org.ehcache.clustered.ChainUtils.chainOf;
import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.ehcache.clustered.Matchers.matchesChain;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;


public class PassiveReplicationMessageCodecTest {

  private PassiveReplicationMessageCodec codec = new PassiveReplicationMessageCodec();

  @Test
  public void testChainReplicationMessageCodec() {
    Chain chain = chainOf(createPayload(2L), createPayload(20L));
    ChainReplicationMessage chainReplicationMessage = new ChainReplicationMessage(2L, chain, 200L, 100L, 1L);

    byte[] encoded = codec.encode(chainReplicationMessage);
    ChainReplicationMessage decodedMsg = (ChainReplicationMessage) codec.decode(EhcacheMessageType.CHAIN_REPLICATION_OP, wrap(encoded));

    assertThat(decodedMsg.getClientId(), is(chainReplicationMessage.getClientId()));
    assertThat(decodedMsg.getTransactionId(), is(chainReplicationMessage.getTransactionId()));
    assertThat(decodedMsg.getOldestTransactionId(), is(chainReplicationMessage.getOldestTransactionId()));
    assertThat(decodedMsg.getKey(), is(chainReplicationMessage.getKey()));
    assertThat(decodedMsg.getChain(), matchesChain(chainReplicationMessage.getChain()));

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
