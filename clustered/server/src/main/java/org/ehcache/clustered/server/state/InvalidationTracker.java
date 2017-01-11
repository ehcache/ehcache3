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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

@CommonComponent
public class InvalidationTracker {

  private final ConcurrentMap<Long, Integer> invalidationMap = new ConcurrentHashMap<>();
  private final AtomicBoolean isClearInProgress = new AtomicBoolean(false);

  public boolean isClearInProgress() {
    return isClearInProgress.get();
  }

  public void setClearInProgress(boolean clearInProgress) {
    isClearInProgress.getAndSet(clearInProgress);
  }

  public ConcurrentMap<Long, Integer> getInvalidationMap() {
    return invalidationMap;
  }

}
