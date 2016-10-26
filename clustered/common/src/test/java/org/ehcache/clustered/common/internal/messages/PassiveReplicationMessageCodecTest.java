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

import org.ehcache.clustered.common.internal.messages.PassiveReplicationMessage.ChainReplicationMessage;
import org.ehcache.clustered.common.internal.messages.PassiveReplicationMessage.ClearInvalidationCompleteMessage;
import org.ehcache.clustered.common.internal.messages.PassiveReplicationMessage.ClientIDTrackerMessage;
import org.ehcache.clustered.common.internal.messages.PassiveReplicationMessage.InvalidationCompleteMessage;
import org.ehcache.clustered.common.internal.store.Chain;
import org.junit.Test;

import java.util.UUID;

import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.ehcache.clustered.common.internal.store.Util.chainsEqual;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class PassiveReplicationMessageCodecTest {

  @Test
  public void testClientIDTrackerMessageCodec() {
    ClientIDTrackerMessage clientIDTrackerMessage = new ClientIDTrackerMessage(200L, UUID.randomUUID());

    PassiveReplicationMessageCodec passiveReplicationMessageCodec = new PassiveReplicationMessageCodec();

    PassiveReplicationMessage decodedMsg = (PassiveReplicationMessage)passiveReplicationMessageCodec.decode(passiveReplicationMessageCodec
        .encode(clientIDTrackerMessage));

    assertThat(decodedMsg.getClientId(), is(clientIDTrackerMessage.getClientId()));
    assertThat(decodedMsg.getId(), is(clientIDTrackerMessage.getId()));

  }

  @Test
  public void testChainReplicationMessageCodec() {
    Chain chain = getChain(false, createPayload(2L), createPayload(20L));
    ChainReplicationMessage chainReplicationMessage = new ChainReplicationMessage("test", 2L, chain, 200L, UUID.randomUUID());

    PassiveReplicationMessageCodec passiveReplicationMessageCodec = new PassiveReplicationMessageCodec();

    ChainReplicationMessage decodedMsg = (ChainReplicationMessage)passiveReplicationMessageCodec.decode(passiveReplicationMessageCodec
        .encode(chainReplicationMessage));

    assertThat(decodedMsg.getCacheId(), is(chainReplicationMessage.getCacheId()));
    assertThat(decodedMsg.getClientId(), is(chainReplicationMessage.getClientId()));
    assertThat(decodedMsg.getId(), is(chainReplicationMessage.getId()));
    assertThat(decodedMsg.getKey(), is(chainReplicationMessage.getKey()));
    assertTrue(chainsEqual(decodedMsg.getChain(), chainReplicationMessage.getChain()));

  }

  @Test
  public void testClearInvalidationCompleteMessage() {
    ClearInvalidationCompleteMessage clearInvalidationCompleteMessage = new ClearInvalidationCompleteMessage("test");

    PassiveReplicationMessageCodec messageCodec = new PassiveReplicationMessageCodec();

    ClearInvalidationCompleteMessage decoded = (ClearInvalidationCompleteMessage)messageCodec.decode(messageCodec.encode(clearInvalidationCompleteMessage));

    assertThat(decoded.getOpCode(), equalTo(clearInvalidationCompleteMessage.getOpCode()));
    assertThat(decoded.getCacheId(), equalTo(clearInvalidationCompleteMessage.getCacheId()));

  }

  @Test
  public void testInvalidationCompleteMessage() {

    InvalidationCompleteMessage invalidationCompleteMessage = new InvalidationCompleteMessage("test", 20L);

    PassiveReplicationMessageCodec messageCodec = new PassiveReplicationMessageCodec();

    InvalidationCompleteMessage decoded = (InvalidationCompleteMessage)messageCodec.decode(messageCodec.encode(invalidationCompleteMessage));

    assertThat(decoded.getOpCode(), equalTo(invalidationCompleteMessage.getOpCode()));
    assertThat(decoded.getCacheId(), equalTo(invalidationCompleteMessage.getCacheId()));
    assertThat(decoded.getKey(), equalTo(invalidationCompleteMessage.getKey()));
  }

}
