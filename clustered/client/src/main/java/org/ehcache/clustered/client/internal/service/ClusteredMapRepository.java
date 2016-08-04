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

package org.ehcache.clustered.client.internal.service;

import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.entity.map.common.ConcurrentClusteredMap;

import java.util.concurrent.ConcurrentMap;

class ClusteredMapRepository {

  private final ConcurrentMap<String, ConcurrentClusteredMap> knownMaps;

  ClusteredMapRepository() {
    knownMaps = new ConcurrentHashMap<String, ConcurrentClusteredMap>();
  }

  ConcurrentClusteredMap getMap(String name) {
    return knownMaps.get(name);
  }

  void addNewMap(String name, ConcurrentClusteredMap map) {
    ConcurrentClusteredMap previous = knownMaps.putIfAbsent(name, map);
    if (previous != null) {
      map.close();
    }
  }

  void clear() {
    for (ConcurrentClusteredMap map : knownMaps.values()) {
      map.close();
    }
    knownMaps.clear();
  }
}
