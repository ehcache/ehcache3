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

import org.assertj.core.util.Maps;
import org.assertj.core.util.Sets;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheResponseType;
import org.ehcache.clustered.server.TestClientSourceId;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.client.message.tracker.OOOMessageHandler;
import org.terracotta.entity.ClientSourceId;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EhcacheMessageTrackerMessageTest {

  public static class NullResponse extends EhcacheEntityResponse {
    @Override
    public EhcacheResponseType getResponseType() {
      return null;
    }
  }

  private EhcacheMessageTrackerMessage message;

  private TestClientSourceId id1 = new TestClientSourceId(1);
  private TestClientSourceId id2 = new TestClientSourceId(2);

  private EhcacheEntityResponse r3 = new NullResponse();
  private EhcacheEntityResponse r4 = new NullResponse();
  private EhcacheEntityResponse r5 = new NullResponse();

  @Before
  public void before() {
    @SuppressWarnings("unchecked")
    OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse> messageHandler = mock(OOOMessageHandler.class);

    Set<ClientSourceId> clientSourceIds = Sets.newLinkedHashSet(id1, id2);
    when(messageHandler.getTrackedClients()).thenReturn(clientSourceIds);

    Map<Long, EhcacheEntityResponse> res1 = Maps.newHashMap();
    res1.put(3L, r3);
    res1.put(4L, r4);
    when(messageHandler.getTrackedResponses(id1)).thenReturn(res1);

    Map<Long, EhcacheEntityResponse> res2 = Maps.newHashMap();
    res2.put(5L, r5);
    when(messageHandler.getTrackedResponses(id2)).thenReturn(res2);

    message = new EhcacheMessageTrackerMessage(messageHandler);
  }

  @Test
  public void getMessageType() throws Exception {
    assertThat(message.getMessageType()).isEqualTo(SyncMessageType.MESSAGE_TRACKER);
  }

  @Test
  public void getTrackedMessages() throws Exception {
    Map<Long, Map<Long, EhcacheEntityResponse>> result = message.getTrackedMessages();
    assertThat(result).containsKeys(id1.toLong(), id2.toLong());

    assertThat(result.get(id1.toLong())).contains(entry(3L, r3), entry(4L, r4));
    assertThat(result.get(id2.toLong())).contains(entry(5L, r5));
  }

}
