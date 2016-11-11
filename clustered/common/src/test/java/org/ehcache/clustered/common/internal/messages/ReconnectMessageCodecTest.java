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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class ReconnectMessageCodecTest {

  @Test
  public void testCodec() {

    Set<String> caches = new HashSet<String>();
    caches.add("test");
    caches.add("test1");
    caches.add("test2");

    ReconnectMessage reconnectMessage = new ReconnectMessage(UUID.randomUUID(), caches);

    Set<Long> firstSetToInvalidate = new HashSet<Long>();
    firstSetToInvalidate.add(1L);
    firstSetToInvalidate.add(11L);
    firstSetToInvalidate.add(111L);

    Set<Long> secondSetToInvalidate = new HashSet<Long>();
    secondSetToInvalidate.add(2L);
    secondSetToInvalidate.add(22L);
    secondSetToInvalidate.add(222L);
    secondSetToInvalidate.add(2222L);
    reconnectMessage.addInvalidationsInProgress("test", firstSetToInvalidate);
    reconnectMessage.addInvalidationsInProgress("test1", Collections.<Long>emptySet());
    reconnectMessage.addInvalidationsInProgress("test2", secondSetToInvalidate);
    reconnectMessage.addClearInProgress("test");

    ReconnectMessageCodec dataCodec = new ReconnectMessageCodec();

    ReconnectMessage decoded = dataCodec.decode(dataCodec.encode(reconnectMessage));
    assertThat(decoded, notNullValue());
    assertThat(decoded.getClientId(), is(reconnectMessage.getClientId()));
    assertThat(decoded.getAllCaches(), containsInAnyOrder("test", "test1", "test2"));
    assertThat(decoded.getInvalidationsInProgress("test"), containsInAnyOrder(firstSetToInvalidate.toArray()));
    assertThat(decoded.getInvalidationsInProgress("test1").isEmpty(), is(true));
    assertThat(decoded.getInvalidationsInProgress("test2"), containsInAnyOrder(secondSetToInvalidate.toArray()));
    assertThat(decoded.isClearInProgress("test"), is(true));
    assertThat(decoded.isClearInProgress("test1"), is(false));
    assertThat(decoded.isClearInProgress("test2"), is(false));
  }
}
