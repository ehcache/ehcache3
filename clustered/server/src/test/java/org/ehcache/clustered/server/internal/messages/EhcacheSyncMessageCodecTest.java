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

import org.assertj.core.util.Sets;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.ResponseCodec;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.server.TestClientSourceId;
import org.junit.Test;
import org.terracotta.client.message.tracker.OOOMessageHandler;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.clustered.common.internal.store.Util.chainsEqual;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EhcacheSyncMessageCodecTest {

  private ResponseCodec responseCodec = mock(ResponseCodec.class);

  private EhcacheSyncMessageCodec codec = new EhcacheSyncMessageCodec(responseCodec);

  @Test
  public void testDataSyncMessageEncodeDecode() throws Exception {
    Map<Long, Chain> chainMap = new HashMap<>();
    Chain chain = getChain(true, createPayload(10L), createPayload(100L), createPayload(1000L));
    chainMap.put(1L, chain);
    chainMap.put(2L, chain);
    chainMap.put(3L, chain);
    EhcacheDataSyncMessage message = new EhcacheDataSyncMessage(chainMap);
    byte[] encodedMessage = codec.encode(0, message);
    EhcacheDataSyncMessage decoded = (EhcacheDataSyncMessage) codec.decode(0, encodedMessage);
    Map<Long, Chain> decodedChainMap = decoded.getChainMap();
    assertThat(decodedChainMap).hasSize(3);
    assertThat(chainsEqual(decodedChainMap.get(1L), chain)).isTrue();
    assertThat(chainsEqual(decodedChainMap.get(2L), chain)).isTrue();
    assertThat(chainsEqual(decodedChainMap.get(3L), chain)).isTrue();
  }

  @Test
  public void testMessageTrackerSyncEncodeDecode_emptyMessage() throws Exception {
    @SuppressWarnings("unchecked")
    OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse> handler = mock(OOOMessageHandler.class);
    when(handler.getTrackedClients()).thenReturn(Collections.emptySet());

    EhcacheMessageTrackerMessage message = new EhcacheMessageTrackerMessage(handler);
    byte[] encodedMessage = codec.encode(0, message);
    EhcacheMessageTrackerMessage decoded = (EhcacheMessageTrackerMessage) codec.decode(0, encodedMessage);
    assertThat(decoded.getTrackedMessages()).isEmpty();
  }

  @Test
  public void testMessageTrackerSyncEncodeDecode_clientWithoutMessage() throws Exception {
    @SuppressWarnings("unchecked")
    OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse> handler = mock(OOOMessageHandler.class);
    when(handler.getTrackedClients()).thenReturn(Collections.singleton(new TestClientSourceId(1)));

    EhcacheMessageTrackerMessage message = new EhcacheMessageTrackerMessage(handler);
    byte[] encodedMessage = codec.encode(0, message);
    EhcacheMessageTrackerMessage decoded = (EhcacheMessageTrackerMessage) codec.decode(0, encodedMessage);
    assertThat(decoded.getTrackedMessages()).isEmpty();
  }

  @Test
  public void testMessageTrackerSyncEncodeDecode_messages() throws Exception {
    TestClientSourceId id1 = new TestClientSourceId(1);
    TestClientSourceId id2 = new TestClientSourceId(2);

    EhcacheEntityResponse r3 = new EhcacheMessageTrackerMessageTest.NullResponse();
    EhcacheEntityResponse r4 = new EhcacheMessageTrackerMessageTest.NullResponse();
    EhcacheEntityResponse r5 = new EhcacheMessageTrackerMessageTest.NullResponse();

    when(responseCodec.encode(r3)).thenReturn(new byte[3]);
    when(responseCodec.encode(r4)).thenReturn(new byte[4]);
    when(responseCodec.encode(r5)).thenReturn(new byte[5]);

    when(responseCodec.decode(argThat(a -> a != null && a.length == 3))).thenReturn(r3);
    when(responseCodec.decode(argThat(a -> a != null && a.length == 4))).thenReturn(r4);
    when(responseCodec.decode(argThat(a -> a != null && a.length == 5))).thenReturn(r5);

    @SuppressWarnings("unchecked")
    OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse> handler = mock(OOOMessageHandler.class);
    when(handler.getTrackedClients()).thenReturn(Sets.newLinkedHashSet(id1, id2));

    Map<Long, EhcacheEntityResponse> responses1 = new HashMap<>();
    responses1.put(3L, r3);
    responses1.put(4L, r4);
    when(handler.getTrackedResponses(id1)).thenReturn(responses1);

    Map<Long, EhcacheEntityResponse> responses2 = new HashMap<>();
    responses2.put(5L, r5);
    when(handler.getTrackedResponses(id2)).thenReturn(responses2);

    EhcacheMessageTrackerMessage message = new EhcacheMessageTrackerMessage(handler);
    byte[] encodedMessage = codec.encode(0, message);
    EhcacheMessageTrackerMessage decoded = (EhcacheMessageTrackerMessage) codec.decode(0, encodedMessage);

    Map<Long, Map<Long, EhcacheEntityResponse>> trackedMessages = decoded.getTrackedMessages();
    assertThat(trackedMessages).containsKeys(id1.toLong(), id2.toLong());
    assertThat(trackedMessages.get(id1.toLong())).containsEntry(3L, r3);
    assertThat(trackedMessages.get(id1.toLong())).containsEntry(4L, r4);
    assertThat(trackedMessages.get(id2.toLong())).containsEntry(5L, r5);
  }
}
