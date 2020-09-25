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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.ehcache.clustered.common.internal.messages.ConcurrentEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.terracotta.entity.ConcurrencyStrategy;
import org.ehcache.clustered.server.internal.messages.EhcacheMessageTrackerCatchup;

import static java.util.Collections.singleton;

public final class ConcurrencyStrategies {

  public static final int DEFAULT_KEY = 1;

  private ConcurrencyStrategies() {
  }

  public static ConcurrencyStrategy<EhcacheEntityMessage> clusterTierConcurrency(KeySegmentMapper mapper) {
    return new DefaultConcurrencyStrategy(mapper);
  }

  public static ConcurrencyStrategy<EhcacheEntityMessage> clusterTierManagerConcurrency() {
    return CLUSTER_TIER_MANAGER_CONCURRENCY_STRATEGY;
  }

  private static final ConcurrencyStrategy<EhcacheEntityMessage> CLUSTER_TIER_MANAGER_CONCURRENCY_STRATEGY = new ConcurrencyStrategy<EhcacheEntityMessage>() {
    @Override
    public int concurrencyKey(EhcacheEntityMessage message) {
      return DEFAULT_KEY;
    }

    @Override
    public Set<Integer> getKeysForSynchronization() {
      return singleton(DEFAULT_KEY);
    }
  };

  public static class DefaultConcurrencyStrategy implements ConcurrencyStrategy<EhcacheEntityMessage> {
    public static final int DATA_CONCURRENCY_KEY_OFFSET = DEFAULT_KEY + 1;
    public static final int TRACKER_SYNC_KEY = Integer.MAX_VALUE - 1;

    private final KeySegmentMapper mapper;

    public DefaultConcurrencyStrategy(KeySegmentMapper mapper) {
      this.mapper = mapper;
    }

    @Override
    public int concurrencyKey(EhcacheEntityMessage entityMessage) {
      if (entityMessage instanceof ServerStoreOpMessage.GetMessage) {
        return UNIVERSAL_KEY;
      } else if (entityMessage instanceof ConcurrentEntityMessage) {
        ConcurrentEntityMessage concurrentEntityMessage = (ConcurrentEntityMessage) entityMessage;
        return DATA_CONCURRENCY_KEY_OFFSET + mapper.getSegmentForKey(concurrentEntityMessage.concurrencyKey());
      } else if (entityMessage instanceof EhcacheMessageTrackerCatchup) {
        return MANAGEMENT_KEY;
      } else {
        return DEFAULT_KEY;
      }
    }

    @Override
    public Set<Integer> getKeysForSynchronization() {
      Set<Integer> result = new LinkedHashSet<>();
      for (int i = 0; i <= mapper.getSegments(); i++) {
        result.add(DEFAULT_KEY + i);
      }
      result.add(TRACKER_SYNC_KEY);
      return Collections.unmodifiableSet(result);
    }
  }
}
