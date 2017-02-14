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

import static java.nio.ByteBuffer.wrap;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.ehcache.clustered.common.internal.store.Util.readPayLoad;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ServerStoreOpCodecTest {

  private static final UUID CLIENT_ID = UUID.randomUUID();
  private static final ServerStoreMessageFactory MESSAGE_FACTORY = new ServerStoreMessageFactory(CLIENT_ID);
  private static final ServerStoreOpCodec STORE_OP_CODEC = new ServerStoreOpCodec();

  @Test
  public void testAppendMessageCodec() {

    ServerStoreOpMessage.AppendMessage appendMessage = MESSAGE_FACTORY.appendOperation(1L, createPayload(1L));
    appendMessage.setId(42L);

    byte[] encoded = STORE_OP_CODEC.encode(appendMessage);
    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(appendMessage.getMessageType(), wrap(encoded));
    ServerStoreOpMessage.AppendMessage decodedAppendMessage = (ServerStoreOpMessage.AppendMessage) decodedMsg;

    assertThat(decodedAppendMessage.getKey(), is(1L));
    assertThat(readPayLoad(decodedAppendMessage.getPayload()), is(1L));
    assertThat(decodedAppendMessage.getId(), is(42L));
    assertThat(decodedAppendMessage.getClientId(), is(CLIENT_ID));
    assertThat(decodedAppendMessage.getMessageType(), is(EhcacheMessageType.APPEND));
  }

  @Test
  public void testGetMessageCodec() {
    ServerStoreOpMessage getMessage = MESSAGE_FACTORY.getOperation(2L);
    getMessage.setId(42L);

    byte[] encoded = STORE_OP_CODEC.encode(getMessage);
    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(getMessage.getMessageType(), wrap(encoded));
    ServerStoreOpMessage.GetMessage decodedGetMessage = (ServerStoreOpMessage.GetMessage) decodedMsg;

    assertThat(decodedGetMessage.getKey(), is(2L));
    assertThat(decodedGetMessage.getId(), is(42L));
    assertThat(decodedGetMessage.getMessageType(), is(EhcacheMessageType.GET_STORE));
    try {
      decodedGetMessage.getClientId();
      fail("AssertionError expected");
    } catch (AssertionError error) {
      assertThat(error.getMessage(), containsString("Client Id is not supported"));
    }

  }

  @Test
  public void testGetAndAppendMessageCodec() {
    ServerStoreOpMessage getAndAppendMessage = MESSAGE_FACTORY.getAndAppendOperation(10L, createPayload(10L));
    getAndAppendMessage.setId(123L);

    byte[] encoded = STORE_OP_CODEC.encode(getAndAppendMessage);
    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(getAndAppendMessage.getMessageType(), wrap(encoded));
    ServerStoreOpMessage.GetAndAppendMessage decodedGetAndAppendMessage = (ServerStoreOpMessage.GetAndAppendMessage) decodedMsg;

    assertThat(decodedGetAndAppendMessage.getKey(), is(10L));
    assertThat(readPayLoad(decodedGetAndAppendMessage.getPayload()), is(10L));
    assertThat(decodedGetAndAppendMessage.getId(), is(123L));
    assertThat(decodedGetAndAppendMessage.getClientId(), is(CLIENT_ID));
    assertThat(decodedGetAndAppendMessage.getMessageType(), is(EhcacheMessageType.GET_AND_APPEND));
  }

  @Test
  public void testReplaceAtHeadMessageCodec() {
    ServerStoreOpMessage replaceAtHeadMessage = MESSAGE_FACTORY.replaceAtHeadOperation(10L,
        getChain(true, createPayload(10L), createPayload(100L), createPayload(1000L)),
        getChain(false, createPayload(2000L)));
    replaceAtHeadMessage.setId(42L);

    byte[] encoded = STORE_OP_CODEC.encode(replaceAtHeadMessage);
    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(replaceAtHeadMessage.getMessageType(), wrap(encoded));
    ServerStoreOpMessage.ReplaceAtHeadMessage decodedReplaceAtHeadMessage = (ServerStoreOpMessage.ReplaceAtHeadMessage) decodedMsg;

    assertThat(decodedReplaceAtHeadMessage.getKey(), is(10L));
    assertThat(decodedReplaceAtHeadMessage.getId(), is(42L));
    Util.assertChainHas(decodedReplaceAtHeadMessage.getExpect(), 10L, 100L, 1000L);
    Util.assertChainHas(decodedReplaceAtHeadMessage.getUpdate(), 2000L);
    assertThat(decodedReplaceAtHeadMessage.getClientId(), is(CLIENT_ID));
    assertThat(decodedReplaceAtHeadMessage.getMessageType(), is(EhcacheMessageType.REPLACE));
  }

  @Test
  public void testClearMessageCodec() throws Exception {
    ServerStoreOpMessage clearMessage = MESSAGE_FACTORY.clearOperation();
    clearMessage.setId(42L);

    byte[] encoded = STORE_OP_CODEC.encode(clearMessage);
    ServerStoreOpMessage decodedMsg = (ServerStoreOpMessage) STORE_OP_CODEC.decode(clearMessage.getMessageType(), wrap(encoded));

    assertThat(decodedMsg.getId(), is(42L));
    assertThat(decodedMsg.getClientId(), is(CLIENT_ID));
    assertThat(decodedMsg.getMessageType(), is(EhcacheMessageType.CLEAR));
  }

  @Test
  public void testClientInvalidationAckMessageCodec() throws Exception {
    ServerStoreOpMessage invalidationAckMessage = MESSAGE_FACTORY.clientInvalidationAck(42L,123);
    invalidationAckMessage.setId(456L);

    byte[] encoded = STORE_OP_CODEC.encode(invalidationAckMessage);
    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(invalidationAckMessage.getMessageType(), wrap(encoded));
    ServerStoreOpMessage.ClientInvalidationAck decodedInvalidationAckMessage = (ServerStoreOpMessage.ClientInvalidationAck)decodedMsg;

    assertThat(decodedInvalidationAckMessage.getKey(), is(42L));
    assertThat(decodedInvalidationAckMessage.getInvalidationId(), is(123));
    assertThat(decodedInvalidationAckMessage.getId(), is(456L));
    assertThat(decodedInvalidationAckMessage.getMessageType(), is(EhcacheMessageType.CLIENT_INVALIDATION_ACK));
    try {
      decodedInvalidationAckMessage.getClientId();
      fail("AssertionError expected");
    } catch (AssertionError error) {
      assertThat(error.getMessage(), containsString("Client Id is not supported"));
    }
  }
}
