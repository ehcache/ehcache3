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

import org.ehcache.clustered.common.internal.store.Element;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

public class ServerStoreOpMessageTest {

  private static final UUID CLIENT_ID = UUID.randomUUID();

  @Test
  public void testConcurrencyKeysEqualForSameCacheAndKey() throws Exception {
    ConcurrentEntityMessage m1 = new ServerStoreOpMessage.AppendMessage(1L, createPayload(1L), CLIENT_ID);
    ConcurrentEntityMessage m2 = new ServerStoreOpMessage.GetAndAppendMessage(1L, createPayload(1L), CLIENT_ID);
    ConcurrentEntityMessage m3 = new ServerStoreOpMessage.ReplaceAtHeadMessage(1L, getChain(Collections.<Element>emptyList()), getChain(Collections.<Element>emptyList()), CLIENT_ID);

    assertThat(m1.concurrencyKey(), is(m2.concurrencyKey()));
    assertThat(m2.concurrencyKey(), is(m3.concurrencyKey()));
  }

  @Test
  public void testConcurrencyKeysEqualForDifferentCachesSameKey() throws Exception {
    ConcurrentEntityMessage m1 = new ServerStoreOpMessage.AppendMessage(1L, createPayload(1L), CLIENT_ID);
    ConcurrentEntityMessage m2 = new ServerStoreOpMessage.GetAndAppendMessage(1L, createPayload(1L), CLIENT_ID);

    assertThat(m1.concurrencyKey(), is(m2.concurrencyKey()));
  }

  @Test
  public void testConcurrencyKeysNotEqualForDifferentCachesAndKeys() throws Exception {
    ConcurrentEntityMessage m1 = new ServerStoreOpMessage.AppendMessage(1L, createPayload(1L), CLIENT_ID);
    ConcurrentEntityMessage m2 = new ServerStoreOpMessage.GetAndAppendMessage(2L, createPayload(1L), CLIENT_ID);
    ConcurrentEntityMessage m3 = new ServerStoreOpMessage.AppendMessage(3L, createPayload(1L), CLIENT_ID);

    assertThat(m1.concurrencyKey(), not(m2.concurrencyKey()));
    assertThat(m1.concurrencyKey(), not(m3.concurrencyKey()));
  }

}
