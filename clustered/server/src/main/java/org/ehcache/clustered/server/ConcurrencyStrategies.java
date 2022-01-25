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
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.terracotta.entity.ConcurrencyStrategy;

import org.terracotta.entity.EntityMessage;

public final class ConcurrencyStrategies {

  private ConcurrencyStrategies() {
  }

  public static final <T extends EntityMessage> ConcurrencyStrategy<T> defaultConcurrency(KeySegmentMapper mapper) {
    return new DefaultConcurrencyStrategy<T>(mapper);
  }

  public static class DefaultConcurrencyStrategy<T extends EntityMessage> implements ConcurrencyStrategy<T> {
    public static final int DEFAULT_KEY = 1;
    public static final int DATA_CONCURRENCY_KEY_OFFSET = DEFAULT_KEY + 1;

    private final KeySegmentMapper mapper;

    public DefaultConcurrencyStrategy(KeySegmentMapper mapper) {
      this.mapper = mapper;
    }

    @Override
    public int concurrencyKey(EntityMessage entityMessage) {
      if (entityMessage instanceof ServerStoreOpMessage.GetMessage) {
        return UNIVERSAL_KEY;
      } else if (entityMessage instanceof ConcurrentEntityMessage) {
        ConcurrentEntityMessage concurrentEntityMessage = (ConcurrentEntityMessage) entityMessage;
        return DATA_CONCURRENCY_KEY_OFFSET + mapper.getSegmentForKey(concurrentEntityMessage.concurrencyKey());
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
      return Collections.unmodifiableSet(result);
    }
  }
}
