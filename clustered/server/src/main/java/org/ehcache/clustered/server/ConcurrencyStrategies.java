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
import java.util.HashSet;
import java.util.Set;

import org.ehcache.clustered.common.internal.messages.ConcurrentEntityMessage;
import org.terracotta.entity.ConcurrencyStrategy;

import org.terracotta.entity.EntityMessage;

public final class ConcurrencyStrategies {

  private ConcurrencyStrategies() {
  }

  public static final <T extends EntityMessage> ConcurrencyStrategy<T> defaultConcurrency(int bucketCount) {
    return new DefaultConcurrencyStrategy<T>(bucketCount);
  }

  static class DefaultConcurrencyStrategy<T extends EntityMessage> implements ConcurrencyStrategy<T> {
    public static final int DEFAULT_KEY = 1;

    private final int bucketCount;

    public DefaultConcurrencyStrategy(int bucketCount) {
      this.bucketCount = bucketCount;
    }

    @Override
    public int concurrencyKey(EntityMessage entityMessage) {
      if (entityMessage instanceof ConcurrentEntityMessage) {
        ConcurrentEntityMessage concurrentEntityMessage = (ConcurrentEntityMessage) entityMessage;
        return DEFAULT_KEY + Math.abs(concurrentEntityMessage.concurrencyKey() % bucketCount);
      }
      return DEFAULT_KEY;
    }

    @Override
    public Set<Integer> getKeysForSynchronization() {
      Set<Integer> result = new HashSet<Integer>();
      for (int i = 0; i < bucketCount; i++) {
        result.add(DEFAULT_KEY + i);
      }
      return Collections.unmodifiableSet(result);
    }
  }
}
