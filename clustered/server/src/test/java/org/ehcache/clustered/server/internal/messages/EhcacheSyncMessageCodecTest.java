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

import org.ehcache.clustered.common.internal.store.Chain;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.ehcache.clustered.common.internal.store.Util.chainsEqual;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class EhcacheSyncMessageCodecTest {

  @Test
  public void testDataSyncMessageEncodeDecode() throws Exception {
    EhcacheSyncMessageCodec codec = new EhcacheSyncMessageCodec();
    Map<Long, Chain> chainMap = new HashMap<>();
    Chain chain = getChain(true, createPayload(10L), createPayload(100L), createPayload(1000L));
    chainMap.put(1L, chain);
    chainMap.put(2L, chain);
    chainMap.put(3L, chain);
    EhcacheDataSyncMessage message = new EhcacheDataSyncMessage(chainMap);
    byte[] encodedMessage = codec.encode(0, message);
    EhcacheDataSyncMessage decoded = (EhcacheDataSyncMessage) codec.decode(0, encodedMessage);
    Map<Long, Chain> decodedChainMap = decoded.getChainMap();
    assertThat(decodedChainMap.size(), is(3));
    assertThat(chainsEqual(decodedChainMap.get(1L), chain), is(true));
    assertThat(chainsEqual(decodedChainMap.get(2L), chain), is(true));
    assertThat(chainsEqual(decodedChainMap.get(3L), chain), is(true));
  }
}
