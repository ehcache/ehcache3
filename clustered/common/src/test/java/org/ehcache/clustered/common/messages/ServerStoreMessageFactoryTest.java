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
package org.ehcache.clustered.common.messages;

import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.ehcache.clustered.common.store.Util.createPayload;
import static org.ehcache.clustered.common.store.Util.readPayLoad;
import static org.ehcache.clustered.common.store.Util.getChain;

public class ServerStoreMessageFactoryTest {

  private static final ServerStoreMessageFactory MESSAGE_FACTORY = new ServerStoreMessageFactory("test");

  @Test
  public void testAppendMessageCodec() {
    EhcacheEntityMessage appendMessage = MESSAGE_FACTORY.appendOperation(1L, createPayload(1L));

    EhcacheEntityMessage decodedMsg = ServerStoreOpCodec.decode(appendMessage.encode());

    assertThat(((ServerStoreOpMessage)decodedMsg).getCacheId(), is("test"));
    assertThat(((ServerStoreOpMessage)decodedMsg).getKey(), is(1L));
    assertThat(readPayLoad(((ServerStoreOpMessage.AppendMessage)decodedMsg).getPayload()), is(1L));
  }

  @Test
  public void testGetMessageCodec() {
    EhcacheEntityMessage getMessage = MESSAGE_FACTORY.getOperation(2L);

    EhcacheEntityMessage decodedMsg = ServerStoreOpCodec.decode(getMessage.encode());

    assertThat(((ServerStoreOpMessage)decodedMsg).getCacheId(), is("test"));
    assertThat(((ServerStoreOpMessage)decodedMsg).getKey(), is(2L));
  }

  @Test
  public void testGetAndAppendMessageCodec() {
    EhcacheEntityMessage getAndAppendMessage = MESSAGE_FACTORY.getAndAppendOperation(10L, createPayload(10L));

    EhcacheEntityMessage decodedMsg = ServerStoreOpCodec.decode(getAndAppendMessage.encode());

    assertThat(((ServerStoreOpMessage)decodedMsg).getCacheId(), is("test"));
    assertThat(((ServerStoreOpMessage)decodedMsg).getKey(), is(10L));
    assertThat(readPayLoad(((ServerStoreOpMessage.GetAndAppendMessage)decodedMsg).getPayload()), is(10L));
  }

  @Test
  public void testReplaceAtHeadMessageCodec() {
    EhcacheEntityMessage replaceAtHeadMessage = MESSAGE_FACTORY.replaceAtHeadOperation(10L,
        getChain(true, createPayload(10L), createPayload(100L), createPayload(1000L)),
        getChain(false, createPayload(2000L)));

    EhcacheEntityMessage decodedMsg = ServerStoreOpCodec.decode(replaceAtHeadMessage.encode());

    assertThat(((ServerStoreOpMessage)decodedMsg).getCacheId(), is("test"));
    assertThat(((ServerStoreOpMessage)decodedMsg).getKey(), is(10L));
    Util.assertChainHas(((ServerStoreOpMessage.ReplaceAtHeadMessage)decodedMsg).getExpect(), 10L, 100L, 1000L);
    Util.assertChainHas(((ServerStoreOpMessage.ReplaceAtHeadMessage)decodedMsg).getUpdate(), 2000L);
  }

}
