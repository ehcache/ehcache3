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

package org.ehcache.clustered.server.repo;

import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.IllegalMessageException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class ServerStateRepository {

  private final ConcurrentMap<String, ConcurrentMap> concurrentMapRepo = new ConcurrentHashMap<String, ConcurrentMap>();

  EhcacheEntityResponse invoke(StateRepositoryOpMessage message) throws ClusterException {
    String mapId = message.getMapId();
    ConcurrentMap<Object, Object> map = concurrentMapRepo.get(mapId);
    if (map == null) {
      ConcurrentHashMap newMap = new ConcurrentHashMap();
      map = concurrentMapRepo.putIfAbsent(mapId, newMap);
      if (map == null) {
        map = newMap;
      }
    }

    Object result = null;
    switch (message.operation()) {
      case GET:
        StateRepositoryOpMessage.GetMessage getMessage = (StateRepositoryOpMessage.GetMessage) message;
        result = map.get(getMessage.getKey());
        break;
      case PUT_IF_ABSENT:
        StateRepositoryOpMessage.PutIfAbsentMessage putIfAbsentMessage = (StateRepositoryOpMessage.PutIfAbsentMessage) message;
        result = map.putIfAbsent(putIfAbsentMessage.getKey(), putIfAbsentMessage.getValue());
        break;
      case ENTRY_SET:
        Set<Map.Entry> entrySet = new HashSet<Map.Entry>();
        for (Map.Entry entry : map.entrySet()) {
          entrySet.add(new AbstractMap.SimpleEntry(entry.getKey(), entry.getValue()));
        }
        result = entrySet;
        break;
      default:
        throw new IllegalMessageException("Invalid operation: " + message.operation());
    }
    return EhcacheEntityResponse.mapValue(result);
  }
}
