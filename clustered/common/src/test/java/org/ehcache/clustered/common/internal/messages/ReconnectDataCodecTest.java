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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class ReconnectDataCodecTest {

  @Test
  public void testCodec() {
    ReconnectData reconnectData = new ReconnectData();
    reconnectData.add("test");
    reconnectData.add("test1");
    reconnectData.add("test2");

    reconnectData.setClientId(UUID.randomUUID());

    Set<Long> firstSetToInvalidate = new HashSet<Long>();
    firstSetToInvalidate.add(1L);
    firstSetToInvalidate.add(11L);
    firstSetToInvalidate.add(111L);

    Set<Long> secondSetToInvalidate = new HashSet<Long>();
    secondSetToInvalidate.add(2L);
    secondSetToInvalidate.add(22L);
    secondSetToInvalidate.add(222L);
    secondSetToInvalidate.add(2222L);
    reconnectData.addInvalidationsInProgress("test", firstSetToInvalidate);
    reconnectData.addInvalidationsInProgress("test1", Collections.EMPTY_SET);
    reconnectData.addInvalidationsInProgress("test2", secondSetToInvalidate);

    ReconnectDataCodec dataCodec = new ReconnectDataCodec();

    ReconnectData decoded = dataCodec.decode(dataCodec.encode(reconnectData));

    assertThat(decoded, notNullValue());
    assertThat(decoded.getClientId(), is(reconnectData.getClientId()));
    assertThat(decoded.getAllCaches(), containsInAnyOrder("test", "test1", "test2"));

    assertThat(decoded.removeInvalidationsInProgress("test"), containsInAnyOrder(firstSetToInvalidate.toArray()));
    assertThat(decoded.removeInvalidationsInProgress("test1").isEmpty(), is(true));
    assertThat(decoded.removeInvalidationsInProgress("test2"), containsInAnyOrder(secondSetToInvalidate.toArray()));

  }
}
