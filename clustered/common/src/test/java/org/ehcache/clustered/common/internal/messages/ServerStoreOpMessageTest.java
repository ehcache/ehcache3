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

import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

/**
 * @author Ludovic Orban
 */
public class ServerStoreOpMessageTest {

  @Test
  public void testConcurrencyKeysEqualForSameCache() throws Exception {
    ServerStoreOpMessage m1 = new ServerStoreOpMessage.ClearMessage("cache1");
    ServerStoreOpMessage m2 = new ServerStoreOpMessage.ClientInvalidationAck("cache1", 1);

    assertThat(m1.concurrencyKey(), is(m2.concurrencyKey()));
  }

  @Test
  public void testConcurrencyKeysEqualForSameCacheAndKey() throws Exception {
    ServerStoreOpMessage m1 = new ServerStoreOpMessage.AppendMessage("cache1", 1L, createPayload(1L));
    ServerStoreOpMessage m2 = new ServerStoreOpMessage.GetAndAppendMessage("cache1", 1L, createPayload(1L));
    ServerStoreOpMessage m3 = new ServerStoreOpMessage.GetMessage("cache1", 1L);
    ServerStoreOpMessage m4 = new ServerStoreOpMessage.ReplaceAtHeadMessage("cache1", 1L, getChain(Collections.<Element>emptyList()), getChain(Collections.<Element>emptyList()));

    assertThat(m1.concurrencyKey(), is(m2.concurrencyKey()));
    assertThat(m2.concurrencyKey(), is(m3.concurrencyKey()));
    assertThat(m3.concurrencyKey(), is(m4.concurrencyKey()));
  }

  @Test
  public void testConcurrencyKeysNotEqualForDifferentCaches() throws Exception {
    ServerStoreOpMessage m1 = new ServerStoreOpMessage.AppendMessage("cache1", 1L, createPayload(1L));
    ServerStoreOpMessage m2 = new ServerStoreOpMessage.GetAndAppendMessage("cache2", 1L, createPayload(1L));

    assertThat(m1.concurrencyKey(), not(m2.concurrencyKey()));
  }

  @Test
  public void testConcurrencyKeysNotEqualForDifferentCachesAndKeys() throws Exception {
    ServerStoreOpMessage m1 = new ServerStoreOpMessage.AppendMessage("cache1", 1L, createPayload(1L));
    ServerStoreOpMessage m2 = new ServerStoreOpMessage.GetAndAppendMessage("cache2", 1L, createPayload(1L));
    ServerStoreOpMessage m3 = new ServerStoreOpMessage.AppendMessage("cache1", 2L, createPayload(1L));

    assertThat(m1.concurrencyKey(), not(m2.concurrencyKey()));
    assertThat(m1.concurrencyKey(), not(m3.concurrencyKey()));
  }

}
