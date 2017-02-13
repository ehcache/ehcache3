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

package org.ehcache.clustered.server.state;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class InvalidationTrackerImpl implements InvalidationTracker {

  private final ConcurrentMap<Long, Integer> invalidationMap = new ConcurrentHashMap<>();
  private final AtomicBoolean isClearInProgress = new AtomicBoolean(false);

  protected ConcurrentMap<Long, Integer> getInvalidationMap() {
    return invalidationMap;
  }

  @Override
  public boolean isClearInProgress() {
    return isClearInProgress.get();
  }

  @Override
  public void setClearInProgress(boolean clearInProgress) {
    isClearInProgress.set(clearInProgress);
  }

  protected long getInvalidationCount(long chainKey) {
    return getInvalidationMap().getOrDefault(chainKey, 0);
  }

  @Override
  public void trackHashInvalidation(long chainKey) {
    getInvalidationMap().compute(chainKey, (key, count) -> {
      if (count == null) {
        return 1;
      } else {
        return count + 1;
      }
    });
  }

  @Override
  public void untrackHashInvalidation(long chainKey) {
    getInvalidationMap().computeIfPresent(chainKey, (key, count) -> {
      if (count == 1) {
        return null;
      }
      return count - 1;
    });
  }

  @Override
  public Set<Long> getTrackedKeys() {
    return getInvalidationMap().keySet();
  }

  @Override
  public void clear() {
    getInvalidationMap().clear();
  }
}
