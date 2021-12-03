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
package org.ehcache.clustered.server;

import org.ehcache.clustered.common.internal.messages.ConcurrentEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.terracotta.entity.ConcurrencyStrategy;

import java.util.HashSet;
import java.util.Set;

import static org.ehcache.clustered.server.ConcurrencyStrategies.DEFAULT_KEY;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.terracotta.entity.ConcurrencyStrategy.UNIVERSAL_KEY;

/**
 * @author Ludovic Orban
 */
public class DefaultConcurrencyStrategyTest {

  private static final KeySegmentMapper DEFAULT_MAPPER = new KeySegmentMapper(16);

  @Test
  public void testConcurrencyKey() throws Exception {
    final int concurrency = 107;
    ConcurrencyStrategy<EhcacheEntityMessage> strategy = ConcurrencyStrategies.clusterTierConcurrency(DEFAULT_MAPPER);

    assertThat(strategy.concurrencyKey(new NonConcurrentTestEntityMessage()), is(DEFAULT_KEY));

    for (int i = -1024; i < 1024; i++) {
      assertThat(strategy.concurrencyKey(new ConcurrentTestEntityMessage(i)), withinRange(DEFAULT_KEY, concurrency));
    }
  }

  @Test
  public void testConcurrencyKeyForServerStoreGetOperation() throws Exception {
    ConcurrencyStrategy<EhcacheEntityMessage> strategy = ConcurrencyStrategies.clusterTierConcurrency(DEFAULT_MAPPER);
    ServerStoreOpMessage.GetMessage getMessage = mock(ServerStoreOpMessage.GetMessage.class);
    assertThat(strategy.concurrencyKey(getMessage), is(UNIVERSAL_KEY));
  }

  @Test
  public void testKeysForSynchronization() throws Exception {
    final int concurrency = 111;
    ConcurrencyStrategy<EhcacheEntityMessage> strategy = ConcurrencyStrategies.clusterTierConcurrency(DEFAULT_MAPPER);

    Set<Integer> visitedConcurrencyKeys = new HashSet<>();
    for (int i = -1024; i < 1024; i++) {
      int concurrencyKey = strategy.concurrencyKey(new ConcurrentTestEntityMessage(i));
      assertThat(concurrencyKey, withinRange(DEFAULT_KEY, concurrency));
      visitedConcurrencyKeys.add(concurrencyKey);
    }
    Set<Integer> keysForSynchronization = strategy.getKeysForSynchronization();
    assertThat(keysForSynchronization.contains(DEFAULT_KEY), is(true));
    assertThat(keysForSynchronization.containsAll(visitedConcurrencyKeys), is(true));
  }

  private static Matcher<Integer> withinRange(int greaterThanOrEqualTo, int lessThan) {
    return allOf(greaterThanOrEqualTo(greaterThanOrEqualTo), lessThan(lessThan));
  }

  private static class NonConcurrentTestEntityMessage extends EhcacheEntityMessage {
  }

  private static class ConcurrentTestEntityMessage extends EhcacheEntityMessage implements ConcurrentEntityMessage {

    private final int key;

    public ConcurrentTestEntityMessage(int key) {
      this.key = key;
    }

    @Override
    public long concurrencyKey() {
      return key;
    }
  }

}
