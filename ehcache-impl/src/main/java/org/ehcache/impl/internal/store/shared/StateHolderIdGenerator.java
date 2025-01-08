/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache.impl.internal.store.shared;

import org.ehcache.spi.persistence.StateHolder;
import org.ehcache.spi.persistence.StateRepository;

import java.io.Serializable;

public class StateHolderIdGenerator<K extends Serializable> {
  private static final Object DEAD_SENTINEL = new Sentinel();
  volatile int lastUsedId;
  final private StateHolder<K, Integer> forwardMap;
  final private StateHolder<Integer, Serializable> reverseMap;

  public StateHolderIdGenerator(StateRepository repository, Class<K> keyClazz) {
    forwardMap = repository.getPersistentStateHolder("forward-map", keyClazz, Integer.class, c -> true, null);
    reverseMap = repository.getPersistentStateHolder("reverse-map", Integer.class, Serializable.class, c -> true, null);
  }

  /***
   * Maps the given key to a unique integer and returns that integer.
   * @param key the value for which a mapped integer will be returned
   * @return the integer value mapped to the key
   */
  public int map(K key) {
    Integer existing = forwardMap.get(key);
    if (existing == null) {
      // key is not mapped, first try to reserve an id for it
      for (int candidate = lastUsedId + 1; ; candidate++) {
        Serializable mappedKey = reverseMap.putIfAbsent(candidate, key);
        if (mappedKey == null || mappedKey.equals(key)) {
          // candidate Id is now mapped to key, but check if the key has been mapped since the call to get() above?
          existing = forwardMap.putIfAbsent(key, candidate);
          if (existing == null || existing.equals(candidate)) {
            // key to candidate Id mapping established
            return lastUsedId = candidate;
          } else {
            reverseMap.remove(candidate, key);
            setLastUsedId(existing);
            return existing;
          }
        }
      }
    } else {
      setLastUsedId(existing);
      return existing;
    }
  }
  private synchronized void setLastUsedId(Integer existing) {
    lastUsedId = Integer.max(existing, lastUsedId);
  }

  public void clear(K key, int id) {
    if (!forwardMap.remove(key, id) || !reverseMap.remove(id, key)) {
      throw new IllegalStateException();
    }
  }

  @SuppressWarnings("unchecked")
  public void purge(K alias) {
    int id = forwardMap.get(alias);

    reverseMap.remove(id, alias);
    reverseMap.putIfAbsent(id, (K) DEAD_SENTINEL);
    forwardMap.remove(alias, id);
  }

  static final class Sentinel implements Serializable {
    private static final long serialVersionUID = 1L;
  }
}
