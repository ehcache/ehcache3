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

import static org.ehcache.clustered.common.messages.Util.createPayload;
import static org.ehcache.clustered.common.messages.Util.readPayLoad;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

public class ServerStoreOpCodecTest {

  @Test
  public void testAppendMessageCodec() {
    EhcacheEntityMessage appendMessage = EhcacheEntityMessage.appendOperation("test", 1L, createPayload(1L));

    EhcacheEntityMessage decodedMsg = ServerStoreOpCodec.decode(ServerStoreOpCodec
        .encode((ServerStoreOpMessage)appendMessage));

    assertThat(((ServerStoreOpMessage)decodedMsg).getCacheId(), is("test"));
    assertThat(((ServerStoreOpMessage)decodedMsg).getKey(), is(1L));
    assertThat(readPayLoad(((ServerStoreOpMessage.AppendMessage)decodedMsg).getPayload()), is(1L));
  }

  @Test
  public void testGetMessageCodec() {
    EhcacheEntityMessage getMessage = EhcacheEntityMessage.getOperation("test", 2L);

    EhcacheEntityMessage decodedMsg = ServerStoreOpCodec.decode(ServerStoreOpCodec
        .encode((ServerStoreOpMessage)getMessage));

    assertThat(((ServerStoreOpMessage)decodedMsg).getCacheId(), is("test"));
    assertThat(((ServerStoreOpMessage)decodedMsg).getKey(), is(2L));
  }

  @Test
  public void testGetAndAppendMessageCodec() {
    EhcacheEntityMessage getAndAppendMessage = EhcacheEntityMessage.getAndAppendOperation("test", 10L, createPayload(10L));

    EhcacheEntityMessage decodedMsg = ServerStoreOpCodec.decode(ServerStoreOpCodec
        .encode((ServerStoreOpMessage)getAndAppendMessage));

    assertThat(((ServerStoreOpMessage)decodedMsg).getCacheId(), is("test"));
    assertThat(((ServerStoreOpMessage)decodedMsg).getKey(), is(10L));
    assertThat(readPayLoad(((ServerStoreOpMessage.GetAndAppendMessage)decodedMsg).getPayload()), is(10L));
  }


}
