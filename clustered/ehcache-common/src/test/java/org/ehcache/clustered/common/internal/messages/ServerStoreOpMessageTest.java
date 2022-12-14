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

import static org.ehcache.clustered.ChainUtils.chainOf;
import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

public class ServerStoreOpMessageTest {

  @Test
  public void testConcurrencyKeysEqualForSameCacheAndKey() {
    ConcurrentEntityMessage m1 = new ServerStoreOpMessage.AppendMessage(1L, createPayload(1L));
    ConcurrentEntityMessage m2 = new ServerStoreOpMessage.GetAndAppendMessage(1L, createPayload(1L));
    ConcurrentEntityMessage m3 = new ServerStoreOpMessage.ReplaceAtHeadMessage(1L, chainOf(), chainOf());

    assertThat(m1.concurrencyKey(), is(m2.concurrencyKey()));
    assertThat(m2.concurrencyKey(), is(m3.concurrencyKey()));
  }

  @Test
  public void testConcurrencyKeysEqualForDifferentCachesSameKey() {
    ConcurrentEntityMessage m1 = new ServerStoreOpMessage.AppendMessage(1L, createPayload(1L));
    ConcurrentEntityMessage m2 = new ServerStoreOpMessage.GetAndAppendMessage(1L, createPayload(1L));

    assertThat(m1.concurrencyKey(), is(m2.concurrencyKey()));
  }

  @Test
  public void testConcurrencyKeysNotEqualForDifferentCachesAndKeys() {
    ConcurrentEntityMessage m1 = new ServerStoreOpMessage.AppendMessage(1L, createPayload(1L));
    ConcurrentEntityMessage m2 = new ServerStoreOpMessage.GetAndAppendMessage(2L, createPayload(1L));
    ConcurrentEntityMessage m3 = new ServerStoreOpMessage.AppendMessage(3L, createPayload(1L));

    assertThat(m1.concurrencyKey(), not(m2.concurrencyKey()));
    assertThat(m1.concurrencyKey(), not(m3.concurrencyKey()));
  }

}
