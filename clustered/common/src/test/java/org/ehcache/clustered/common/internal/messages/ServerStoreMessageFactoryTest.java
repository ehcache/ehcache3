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

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.readPayLoad;
import static org.ehcache.clustered.common.internal.store.Util.getChain;

public class ServerStoreMessageFactoryTest {

  private static final ServerStoreMessageFactory MESSAGE_FACTORY = new ServerStoreMessageFactory("test");

  @Test
  public void testAppendMessage() {
    ServerStoreOpMessage.AppendMessage appendMessage =
        (ServerStoreOpMessage.AppendMessage) MESSAGE_FACTORY.appendOperation(1L, createPayload(1L));

    assertThat(appendMessage.getCacheId(), is("test"));
    assertThat(appendMessage.getKey(), is(1L));
    assertThat(readPayLoad(appendMessage.getPayload()), is(1L));
  }

  @Test
  public void testGetMessage() {
    ServerStoreOpMessage.GetMessage getMessage = (ServerStoreOpMessage.GetMessage) MESSAGE_FACTORY.getOperation(2L);

    assertThat(getMessage.getCacheId(), is("test"));
    assertThat(getMessage.getKey(), is(2L));
  }

  @Test
  public void testGetAndAppendMessage() {
    ServerStoreOpMessage.GetAndAppendMessage getAndAppendMessage =
        (ServerStoreOpMessage.GetAndAppendMessage) MESSAGE_FACTORY.getAndAppendOperation(10L, createPayload(10L));

    assertThat(getAndAppendMessage.getCacheId(), is("test"));
    assertThat(getAndAppendMessage.getKey(), is(10L));
    assertThat(readPayLoad(getAndAppendMessage.getPayload()), is(10L));
  }

  @Test
  public void testReplaceAtHeadMessage() {
    ServerStoreOpMessage.ReplaceAtHeadMessage replaceAtHeadMessage =
        (ServerStoreOpMessage.ReplaceAtHeadMessage) MESSAGE_FACTORY.replaceAtHeadOperation(10L,
            getChain(true, createPayload(10L), createPayload(100L), createPayload(1000L)),
            getChain(false, createPayload(2000L))
        );

    assertThat(replaceAtHeadMessage.getCacheId(), is("test"));
    assertThat(replaceAtHeadMessage.getKey(), is(10L));
    Util.assertChainHas(replaceAtHeadMessage.getExpect(), 10L, 100L, 1000L);
    Util.assertChainHas(replaceAtHeadMessage.getUpdate(), 2000L);
  }

  @Test
  public void testClearMessage() throws Exception {
    ServerStoreOpMessage.ClearMessage clearMessage = (ServerStoreOpMessage.ClearMessage) MESSAGE_FACTORY.clearOperation();
    assertThat(clearMessage.getCacheId(), is("test"));
  }

  @Test
  public void testClientInvalidationAckMessage() throws Exception {
    ServerStoreOpMessage.ClientInvalidationAck invalidationAck =
        (ServerStoreOpMessage.ClientInvalidationAck) MESSAGE_FACTORY.clientInvalidationAck(1234);
    assertThat(invalidationAck.getCacheId(), is("test"));
    assertThat(invalidationAck.getInvalidationId(), is(1234));
  }
}
