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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ReconnectMessage {

  private final UUID clientId;
  private final Set<String> caches;
  private final ConcurrentMap<String, Set<Long>> hashInvalidationsInProgressPerCache = new ConcurrentHashMap<String, Set<Long>>();
  private final Set<String> cachesWithClearInProgress = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  public ReconnectMessage(UUID clientId, Set<String> caches) {
    if (clientId == null) {
      throw new IllegalStateException("ClientID cannot be null");
    }
    this.clientId = clientId;
    this.caches = new HashSet<String>(caches);
  }

  public UUID getClientId() {
    return clientId;
  }

  public Set<String> getAllCaches() {
    return this.caches;
  }

  public void addInvalidationsInProgress(String cacheId, Set<Long> hashInvalidationsInProgress) {
    hashInvalidationsInProgressPerCache.put(cacheId, hashInvalidationsInProgress);
  }

  public Set<Long> getInvalidationsInProgress(String cacheId) {
    Set<Long> hashToInvalidate = hashInvalidationsInProgressPerCache.get(cacheId);
    return hashToInvalidate == null ? Collections.<Long>emptySet() : hashToInvalidate;
  }

  public void addClearInProgress(String cacheId) {
    cachesWithClearInProgress.add(cacheId);
  }

  public boolean isClearInProgress(String cacheId) {
    return cachesWithClearInProgress.contains(cacheId);
  }

}
