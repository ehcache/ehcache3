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

package org.ehcache.impl.serialization;

import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.persistence.StateRepository;

import java.io.Serializable;
import java.util.concurrent.ConcurrentMap;

/**
 * TransientStateRepository
 */
class TransientStateRepository implements StateRepository {

  private ConcurrentMap<String, ConcurrentMap<?, ?>> knownMaps = new ConcurrentHashMap<String, ConcurrentMap<?, ?>>();

  @Override
  @SuppressWarnings("unchecked")
  public <K extends Serializable, V extends Serializable> ConcurrentMap<K, V> getPersistentConcurrentMap(String name, Class<K> keyClass, Class<V> valueClass) {
    ConcurrentMap<K, V> concurrentMap = (ConcurrentMap<K, V>) knownMaps.get(name);
    if (concurrentMap != null) {
      return concurrentMap;
    } else {
      ConcurrentHashMap<K, V> newMap = new ConcurrentHashMap<K, V>();
      concurrentMap = (ConcurrentMap<K, V>) knownMaps.putIfAbsent(name, newMap);
      if (concurrentMap == null) {
        return newMap;
      } else {
        return concurrentMap;
      }
    }
  }
}
