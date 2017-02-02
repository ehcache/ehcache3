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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InvalidationTrackerManagerImpl implements InvalidationTrackerManager {

  private final ConcurrentMap<String, InvalidationTracker> invalidationTrackerMap;

  public InvalidationTrackerManagerImpl() {
    invalidationTrackerMap = new ConcurrentHashMap<>();
  }

  @Override
  public InvalidationTracker getInvalidationTracker(String cacheId) {
    return this.invalidationTrackerMap.get(cacheId);
  }

  @Override
  public void addInvalidationTracker(String cacheId) {
    this.invalidationTrackerMap.put(cacheId, new InvalidationTrackerImpl());
  }

  @Override
  public void removeInvalidationTracker(String cacheId) {
    this.invalidationTrackerMap.remove(cacheId);
  }

  @Override
  public void clear() {
    invalidationTrackerMap.clear();
  }

}
