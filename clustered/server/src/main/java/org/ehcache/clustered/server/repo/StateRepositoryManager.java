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

import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.clustered.server.internal.messages.EhcacheStateRepoSyncMessage;

import com.tc.classloader.CommonComponent;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.emptyList;

@CommonComponent
public class StateRepositoryManager {

  private final ConcurrentMap<String, ServerStateRepository> mapRepositoryMap = new ConcurrentHashMap<>();

  public void destroyStateRepository(String cacheId) {
    mapRepositoryMap.remove(cacheId);
  }

  public EhcacheEntityResponse invoke(StateRepositoryOpMessage message) {
    String cacheId = message.getCacheId();
    ServerStateRepository currentRepo = getServerStateRepository(cacheId);
    return currentRepo.invoke(message);
  }

  private ServerStateRepository getServerStateRepository(String cacheId) {
    ServerStateRepository currentRepo = mapRepositoryMap.get(cacheId);
    if (currentRepo == null) {
      ServerStateRepository newRepo = new ServerStateRepository();
      currentRepo = mapRepositoryMap.putIfAbsent(cacheId, newRepo);
      if (currentRepo == null) {
        currentRepo = newRepo;
      }
    }
    return currentRepo;
  }

  public List<EhcacheStateRepoSyncMessage> syncMessageFor(String cacheId) {
    ServerStateRepository repository = mapRepositoryMap.get(cacheId);
    if (repository != null) {
      return repository.syncMessage(cacheId);
    }
    return emptyList();
  }

  public void processSyncMessage(EhcacheStateRepoSyncMessage stateRepoSyncMessage) {
    getServerStateRepository(stateRepoSyncMessage.getCacheId()).processSyncMessage(stateRepoSyncMessage);
  }
}
