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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ReconnectData {

  private static final byte CLIENT_ID_SIZE = 16;
  private static final byte ENTRY_SIZE = 4;
  private static final byte HASH_SIZE = 8;
  private static final byte CLEAR_IN_PROGRESS_STATUS_SIZE = 1;

  private volatile UUID clientId;
  private final Set<String> reconnectData = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private final AtomicInteger reconnectDatalen = new AtomicInteger(CLIENT_ID_SIZE);
  private final ConcurrentHashMap<String, Set<Long>> hashInvalidationsInProgressPerCache = new ConcurrentHashMap<String, Set<Long>>();
  private final Set<String> cachesWithClearInProgress = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  public UUID getClientId() {
    if (clientId == null) {
      throw new AssertionError("Client ID cannot be null");
    }
    return clientId;
  }

  public void setClientId(UUID clientId) {
    this.clientId = clientId;
  }

  public void add(String name) {
    reconnectData.add(name);
    reconnectDatalen.addAndGet(2 * name.length() + 2 * ENTRY_SIZE + CLEAR_IN_PROGRESS_STATUS_SIZE);
  }

  public void remove(String name) {
    if (!reconnectData.contains(name)) {
      reconnectData.remove(name);
      reconnectDatalen.addAndGet(-(2 * name.length() + 2 * ENTRY_SIZE + CLEAR_IN_PROGRESS_STATUS_SIZE));
    }
  }

  public Set<String> getAllCaches() {
    return Collections.unmodifiableSet(reconnectData);
  }

  int getDataLength() {
    return reconnectDatalen.get();
  }

  public void addInvalidationsInProgress(String cacheId, Set<Long> hashInvalidationsInProgress) {
    hashInvalidationsInProgressPerCache.put(cacheId, hashInvalidationsInProgress);
    reconnectDatalen.addAndGet(hashInvalidationsInProgress.size() * HASH_SIZE);
  }

  public Set<Long> removeInvalidationsInProgress(String cacheId) {
    Set<Long> hashToInvalidate = hashInvalidationsInProgressPerCache.remove(cacheId);
    if (hashToInvalidate != null) { //TODO: while handling eventual
      reconnectDatalen.addAndGet(-(hashToInvalidate.size() * HASH_SIZE));
      return hashToInvalidate;
    }
    return Collections.EMPTY_SET;
  }

  public void addClearInProgress(String cacheId) {
    cachesWithClearInProgress.add(cacheId);
  }

  public Set<String> getClearInProgressCaches() {
    Set<String> caches = new HashSet<String>(cachesWithClearInProgress);
    cachesWithClearInProgress.clear();
    return caches;
  }

}
