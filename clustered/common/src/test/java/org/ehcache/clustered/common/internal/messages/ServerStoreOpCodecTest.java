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

import static java.nio.ByteBuffer.wrap;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.ehcache.clustered.common.internal.store.Util.readPayLoad;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ServerStoreOpCodecTest {

  private static final ServerStoreOpCodec STORE_OP_CODEC = new ServerStoreOpCodec();

  @Test
  public void testAppendMessageCodec() {

    ServerStoreOpMessage.AppendMessage appendMessage = new ServerStoreOpMessage.AppendMessage(1L, createPayload(1L));

    byte[] encoded = STORE_OP_CODEC.encode(appendMessage);
    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(appendMessage.getMessageType(), wrap(encoded));
    ServerStoreOpMessage.AppendMessage decodedAppendMessage = (ServerStoreOpMessage.AppendMessage) decodedMsg;

    assertThat(decodedAppendMessage.getKey(), is(1L));
    assertThat(readPayLoad(decodedAppendMessage.getPayload()), is(1L));
    assertThat(decodedAppendMessage.getMessageType(), is(EhcacheMessageType.APPEND));
  }

  @Test
  public void testGetMessageCodec() {
    ServerStoreOpMessage getMessage = new ServerStoreOpMessage.GetMessage(2L);

    byte[] encoded = STORE_OP_CODEC.encode(getMessage);
    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(getMessage.getMessageType(), wrap(encoded));
    ServerStoreOpMessage.GetMessage decodedGetMessage = (ServerStoreOpMessage.GetMessage) decodedMsg;

    assertThat(decodedGetMessage.getKey(), is(2L));
    assertThat(decodedGetMessage.getMessageType(), is(EhcacheMessageType.GET_STORE));
  }

  @Test
  public void testGetAndAppendMessageCodec() {
    ServerStoreOpMessage getAndAppendMessage = new ServerStoreOpMessage.GetAndAppendMessage(10L, createPayload(10L));

    byte[] encoded = STORE_OP_CODEC.encode(getAndAppendMessage);
    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(getAndAppendMessage.getMessageType(), wrap(encoded));
    ServerStoreOpMessage.GetAndAppendMessage decodedGetAndAppendMessage = (ServerStoreOpMessage.GetAndAppendMessage) decodedMsg;

    assertThat(decodedGetAndAppendMessage.getKey(), is(10L));
    assertThat(readPayLoad(decodedGetAndAppendMessage.getPayload()), is(10L));
    assertThat(decodedGetAndAppendMessage.getMessageType(), is(EhcacheMessageType.GET_AND_APPEND));
  }

  @Test
  public void testReplaceAtHeadMessageCodec() {
    ServerStoreOpMessage replaceAtHeadMessage = new ServerStoreOpMessage.ReplaceAtHeadMessage(10L,
        getChain(true, createPayload(10L), createPayload(100L), createPayload(1000L)),
        getChain(false, createPayload(2000L)));

    byte[] encoded = STORE_OP_CODEC.encode(replaceAtHeadMessage);
    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(replaceAtHeadMessage.getMessageType(), wrap(encoded));
    ServerStoreOpMessage.ReplaceAtHeadMessage decodedReplaceAtHeadMessage = (ServerStoreOpMessage.ReplaceAtHeadMessage) decodedMsg;

    assertThat(decodedReplaceAtHeadMessage.getKey(), is(10L));
    Util.assertChainHas(decodedReplaceAtHeadMessage.getExpect(), 10L, 100L, 1000L);
    Util.assertChainHas(decodedReplaceAtHeadMessage.getUpdate(), 2000L);
    assertThat(decodedReplaceAtHeadMessage.getMessageType(), is(EhcacheMessageType.REPLACE));
  }

  @Test
  public void testClearMessageCodec() throws Exception {
    ServerStoreOpMessage clearMessage = new ServerStoreOpMessage.ClearMessage();

    byte[] encoded = STORE_OP_CODEC.encode(clearMessage);
    ServerStoreOpMessage decodedMsg = (ServerStoreOpMessage) STORE_OP_CODEC.decode(clearMessage.getMessageType(), wrap(encoded));

    assertThat(decodedMsg.getMessageType(), is(EhcacheMessageType.CLEAR));
  }

  @Test
  public void testClientInvalidationAckMessageCodec() throws Exception {
    ServerStoreOpMessage invalidationAckMessage = new ServerStoreOpMessage.ClientInvalidationAck(42L,123);

    byte[] encoded = STORE_OP_CODEC.encode(invalidationAckMessage);
    EhcacheEntityMessage decodedMsg = STORE_OP_CODEC.decode(invalidationAckMessage.getMessageType(), wrap(encoded));
    ServerStoreOpMessage.ClientInvalidationAck decodedInvalidationAckMessage = (ServerStoreOpMessage.ClientInvalidationAck)decodedMsg;

    assertThat(decodedInvalidationAckMessage.getKey(), is(42L));
    assertThat(decodedInvalidationAckMessage.getInvalidationId(), is(123));
    assertThat(decodedInvalidationAckMessage.getMessageType(), is(EhcacheMessageType.CLIENT_INVALIDATION_ACK));
  }

  @Test
  public void testLockMessage() throws Exception {
    ServerStoreOpMessage lockMessage = new ServerStoreOpMessage.LockMessage(2L);

    byte[] encoded = STORE_OP_CODEC.encode(lockMessage);
    EhcacheEntityMessage decoded = STORE_OP_CODEC.decode(lockMessage.getMessageType(), wrap(encoded));

    ServerStoreOpMessage.LockMessage decodedLockMessage = (ServerStoreOpMessage.LockMessage) decoded;

    assertThat(decodedLockMessage.getHash(), is(2L));
    assertThat(decodedLockMessage.getMessageType(), is(EhcacheMessageType.LOCK));
  }

  @Test
  public void testUnlockMessage() throws Exception {
    ServerStoreOpMessage unlockMessage = new ServerStoreOpMessage.UnlockMessage(2L);

    byte[] encoded = STORE_OP_CODEC.encode(unlockMessage);
    EhcacheEntityMessage decoded = STORE_OP_CODEC.decode(unlockMessage.getMessageType(), wrap(encoded));

    ServerStoreOpMessage.UnlockMessage decodedLockMessage = (ServerStoreOpMessage.UnlockMessage) decoded;

    assertThat(decodedLockMessage.getHash(), is(2L));
    assertThat(decodedLockMessage.getMessageType(), is(EhcacheMessageType.UNLOCK));
  }

}
