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
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.clustered.server.internal.messages.EhcacheStateRepoSyncMessage;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

class ServerStateRepository {

  private final ConcurrentMap<String, ConcurrentMap<Object, Object>> concurrentMapRepo = new ConcurrentHashMap<>();

  EhcacheEntityResponse invoke(StateRepositoryOpMessage message) {
    String mapId = message.getMapId();
    ConcurrentMap<Object, Object> map = getStateMap(mapId);

    Object result;
    switch (message.getMessageType()) {
      case GET_STATE_REPO:
        StateRepositoryOpMessage.GetMessage getMessage = (StateRepositoryOpMessage.GetMessage) message;
        result = map.get(getMessage.getKey());
        break;
      case PUT_IF_ABSENT:
        StateRepositoryOpMessage.PutIfAbsentMessage putIfAbsentMessage = (StateRepositoryOpMessage.PutIfAbsentMessage) message;
        result = map.putIfAbsent(putIfAbsentMessage.getKey(), putIfAbsentMessage.getValue());
        break;
      case ENTRY_SET:
        result = map.entrySet()
          .stream()
          .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()))
          .collect(Collectors.toSet());
        break;
      default:
        throw new AssertionError("Unsupported operation: " + message.getMessageType());
    }
    return EhcacheEntityResponse.mapValue(result);
  }

  private ConcurrentMap<Object, Object> getStateMap(String mapId) {
    ConcurrentMap<Object, Object> map = concurrentMapRepo.get(mapId);
    if (map == null) {
      ConcurrentHashMap<Object, Object> newMap = new ConcurrentHashMap<>();
      map = concurrentMapRepo.putIfAbsent(mapId, newMap);
      if (map == null) {
        map = newMap;
      }
    }
    return map;
  }

  List<EhcacheStateRepoSyncMessage> syncMessage(String cacheId) {
    ArrayList<EhcacheStateRepoSyncMessage> result = new ArrayList<>();
    for (Map.Entry<String, ConcurrentMap<Object, Object>> entry : concurrentMapRepo.entrySet()) {
      result.add(new EhcacheStateRepoSyncMessage(cacheId, entry.getKey(), entry.getValue()));
    }
    return result;
  }

  void processSyncMessage(EhcacheStateRepoSyncMessage stateRepoSyncMessage) {
    concurrentMapRepo.put(stateRepoSyncMessage.getMapId(), stateRepoSyncMessage.getMappings());
  }
}
