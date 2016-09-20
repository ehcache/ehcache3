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

import org.ehcache.clustered.common.internal.messages.RetirementMessage.ServerStoreRetirementMessage;
import org.ehcache.clustered.common.internal.store.Chain;
import org.junit.Test;

import java.util.UUID;

import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.ehcache.clustered.common.internal.store.Util.chainsEqual;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class RetirementMessageCodecTest {

  @Test
  public void testRetireMessageCodec() {
    RetirementMessage retirementMessage = new RetirementMessage(200L, UUID.randomUUID());

    RetirementMessageCodec retirementMessageCodec = new RetirementMessageCodec();

    RetirementMessage decodedMsg = (RetirementMessage)retirementMessageCodec.decode(retirementMessageCodec.encode(retirementMessage));

    assertThat(decodedMsg.getClientId(), is(retirementMessage.getClientId()));
    assertThat(decodedMsg.getId(), is(retirementMessage.getId()));

  }

  @Test
  public void testServerStoreRetireMessageCodec() {
    Chain chain = getChain(false, createPayload(2L), createPayload(20L));
    ServerStoreRetirementMessage retirementMessage = new ServerStoreRetirementMessage("test", 2L, chain, 200L, UUID.randomUUID());

    RetirementMessageCodec retirementMessageCodec = new RetirementMessageCodec();

    ServerStoreRetirementMessage decodedMsg = (ServerStoreRetirementMessage)retirementMessageCodec.decode(retirementMessageCodec.encode(retirementMessage));

    assertThat(decodedMsg.getCacheId(), is(retirementMessage.getCacheId()));
    assertThat(decodedMsg.getClientId(), is(retirementMessage.getClientId()));
    assertThat(decodedMsg.getId(), is(retirementMessage.getId()));
    assertThat(decodedMsg.getKey(), is(retirementMessage.getKey()));
    assertTrue(chainsEqual(decodedMsg.getChain(), retirementMessage.getChain()));

  }

}
