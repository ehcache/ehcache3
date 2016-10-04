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

import com.tc.classloader.CommonComponent;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks all the evicted keys in Passive Entity.
 * Whenever passive evicts a key, the keys is added to the tracker.
 * The same is removed when passive receives a put for that key.
 */
@CommonComponent
public class EvictionTracker {

  private Set<Long> trackedEvictedKeys = Collections.newSetFromMap(new ConcurrentHashMap<>());

  /**
   * Not to be invoked on Active.
   * @param key to be tracked
   */
  public void add(long key) {
    trackedEvictedKeys.add(key);
  }

  /**
   * Not to be invoked on Active.
   * @param key to be removed from tracking
   */
  public boolean remove(long key) {
    return trackedEvictedKeys.remove(key);
  }

  /**
   * To be only invoked by Active so that it can invalidate
   * clients holding those keys.
   * Once invoked on active, it will clear the state of evicted keys.
   * This means subsequent invocations will return an empty set
   * Not to be invoked on Passive.
   * @return a set of keys evicted independently by passive
   */
  public Set<Long> getTrackedEvictedKeys() {
    Set<Long> toReturn = Collections.unmodifiableSet(new HashSet<>(trackedEvictedKeys));
    trackedEvictedKeys.clear();
    return toReturn;
  }

}
