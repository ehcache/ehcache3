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

import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.ehcache.clustered.common.internal.store.Util.readPayLoad;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class ServerStoreOpCodecTest {

  private static final ServerStoreMessageFactory MESSAGE_FACTORY = new ServerStoreMessageFactory("test");
  private static final ServerStoreOpCodec STORE_OP_CODEC = new ServerStoreOpCodec();

  @Test
  public void testAppendMessageCodec() {

    EhcacheEntityMessage appendMessage = MESSAGE_FACTORY.appendOperation(1L, createPayload(1L));

    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(STORE_OP_CODEC.encode((ServerStoreOpMessage)appendMessage));
    ServerStoreOpMessage.AppendMessage decodedAppendMessage = (ServerStoreOpMessage.AppendMessage) decodedMsg;

    assertThat(decodedAppendMessage.getCacheId(), is("test"));
    assertThat(decodedAppendMessage.getKey(), is(1L));
    assertThat(readPayLoad(decodedAppendMessage.getPayload()), is(1L));
  }

  @Test
  public void testGetMessageCodec() {
    EhcacheEntityMessage getMessage = MESSAGE_FACTORY.getOperation(2L);

    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(STORE_OP_CODEC.encode((ServerStoreOpMessage)getMessage));
    ServerStoreOpMessage.GetMessage decodedGetMessage = (ServerStoreOpMessage.GetMessage) decodedMsg;

    assertThat(decodedGetMessage.getCacheId(), is("test"));
    assertThat(decodedGetMessage.getKey(), is(2L));
  }

  @Test
  public void testGetAndAppendMessageCodec() {
    EhcacheEntityMessage getAndAppendMessage = MESSAGE_FACTORY.getAndAppendOperation(10L, createPayload(10L));

    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(STORE_OP_CODEC.encode((ServerStoreOpMessage)getAndAppendMessage));
    ServerStoreOpMessage.GetAndAppendMessage decodedGetAndAppendMessage = (ServerStoreOpMessage.GetAndAppendMessage) decodedMsg;

    assertThat(decodedGetAndAppendMessage.getCacheId(), is("test"));
    assertThat(decodedGetAndAppendMessage.getKey(), is(10L));
    assertThat(readPayLoad(decodedGetAndAppendMessage.getPayload()), is(10L));
  }

  @Test
  public void testReplaceAtHeadMessageCodec() {
    EhcacheEntityMessage replaceAtHeadMessage = MESSAGE_FACTORY.replaceAtHeadOperation(10L,
        getChain(true, createPayload(10L), createPayload(100L), createPayload(1000L)),
        getChain(false, createPayload(2000L)));

    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(STORE_OP_CODEC.encode((ServerStoreOpMessage)replaceAtHeadMessage));
    ServerStoreOpMessage.ReplaceAtHeadMessage decodedReplaceAtHeadMessage = (ServerStoreOpMessage.ReplaceAtHeadMessage) decodedMsg;

    assertThat(decodedReplaceAtHeadMessage.getCacheId(), is("test"));
    assertThat(decodedReplaceAtHeadMessage.getKey(), is(10L));
    Util.assertChainHas(decodedReplaceAtHeadMessage.getExpect(), 10L, 100L, 1000L);
    Util.assertChainHas(decodedReplaceAtHeadMessage.getUpdate(), 2000L);
  }

  @Test
  public void testClearMessageCodec() throws Exception {
    EhcacheEntityMessage clearMessage = MESSAGE_FACTORY.clearOperation();
    byte[] encodedBytes = STORE_OP_CODEC.encode((ServerStoreOpMessage)clearMessage);
    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(encodedBytes);
    assertThat(((ServerStoreOpMessage)decodedMsg).getCacheId(), is("test"));
  }

  @Test
  public void testClientInvalidationAckMessageCodec() throws Exception {
    EhcacheEntityMessage invalidationAckMessage = MESSAGE_FACTORY.clientInvalidationAck(123);
    byte[] encodedBytes = STORE_OP_CODEC.encode((ServerStoreOpMessage)invalidationAckMessage);
    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(encodedBytes);
    ServerStoreOpMessage.ClientInvalidationAck decodedInvalidationAckMessage = (ServerStoreOpMessage.ClientInvalidationAck)decodedMsg;

    assertThat(decodedInvalidationAckMessage.getCacheId(), is("test"));
    assertThat(decodedInvalidationAckMessage.getInvalidationId(), is(123));
  }
}
