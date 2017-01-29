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

import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.readPayLoad;
import static org.ehcache.clustered.common.internal.store.Util.getChain;

public class ServerStoreMessageFactoryTest {

  private static final ServerStoreMessageFactory MESSAGE_FACTORY = new ServerStoreMessageFactory(UUID.randomUUID());

  @Test
  public void testAppendMessage() {
    ServerStoreOpMessage.AppendMessage appendMessage = MESSAGE_FACTORY.appendOperation(1L, createPayload(1L));

    assertThat(appendMessage.getKey(), is(1L));
    assertThat(readPayLoad(appendMessage.getPayload()), is(1L));
  }

  @Test
  public void testGetMessage() {
    ServerStoreOpMessage.GetMessage getMessage = (ServerStoreOpMessage.GetMessage) MESSAGE_FACTORY.getOperation(2L);

    assertThat(getMessage.getKey(), is(2L));
  }

  @Test
  public void testGetAndAppendMessage() {
    ServerStoreOpMessage.GetAndAppendMessage getAndAppendMessage = MESSAGE_FACTORY.getAndAppendOperation(10L, createPayload(10L));

    assertThat(getAndAppendMessage.getKey(), is(10L));
    assertThat(readPayLoad(getAndAppendMessage.getPayload()), is(10L));
  }

  @Test
  public void testReplaceAtHeadMessage() {
    ServerStoreOpMessage.ReplaceAtHeadMessage replaceAtHeadMessage = MESSAGE_FACTORY.replaceAtHeadOperation(10L,
          getChain(true, createPayload(10L), createPayload(100L), createPayload(1000L)),
          getChain(false, createPayload(2000L))
      );

    assertThat(replaceAtHeadMessage.getKey(), is(10L));
    Util.assertChainHas(replaceAtHeadMessage.getExpect(), 10L, 100L, 1000L);
    Util.assertChainHas(replaceAtHeadMessage.getUpdate(), 2000L);
  }

  @Test
  public void testClientInvalidationAckMessage() throws Exception {
    ServerStoreOpMessage.ClientInvalidationAck invalidationAck = MESSAGE_FACTORY.clientInvalidationAck(42L,1234);
    assertThat(invalidationAck.getKey(), is(42L));
    assertThat(invalidationAck.getInvalidationId(), is(1234));
  }
}
