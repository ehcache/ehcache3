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
package org.ehcache.clustered.server.store;

import org.terracotta.entity.ClientDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class LockManagerImpl implements ServerLockManager {

  private final Map<Long, ClientDescriptor> blockedKeys = new ConcurrentHashMap<>();

  @Override
  public boolean lock(long key, ClientDescriptor client) {
    if (blockedKeys.containsKey(key)) {
      return false;
    }
    blockedKeys.put(key, client);
    return true;
  }

  @Override
  public void unlock(long key) {
    blockedKeys.remove(key);
  }

  @Override
  public void createLockStateAfterFailover(ClientDescriptor client, Set<Long> locksHeld) {
    locksHeld.forEach(key -> {
      ClientDescriptor absent = blockedKeys.putIfAbsent(key, client);
      if (absent != null) {
        throw new IllegalStateException("Key is already locked");
      }
    });
  }

  @Override
  public void sweepLocksForClient(ClientDescriptor client, Consumer<List<Long>> removeHeldKeys) {
    Set<Map.Entry<Long, ClientDescriptor>> entries = new HashSet<>(Collections.unmodifiableSet(blockedKeys.entrySet()));
    List<Long> heldKeys = new ArrayList<>();
    entries.forEach(entry -> {
      if (entry.getValue().equals(client)) {
        heldKeys.add(entry.getKey());
        blockedKeys.remove(entry.getKey());
      }
    });

    if (removeHeldKeys != null) {
      removeHeldKeys.accept(heldKeys);
    }
  }
}
