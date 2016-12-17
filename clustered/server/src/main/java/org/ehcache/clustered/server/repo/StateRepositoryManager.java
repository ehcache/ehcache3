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
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;

import com.tc.classloader.CommonComponent;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@CommonComponent
public class StateRepositoryManager {

  private final ConcurrentMap<String, ServerStateRepository> mapRepositoryMap = new ConcurrentHashMap<String, ServerStateRepository>();

  public void destroyStateRepository(String cacheId) throws ClusterException {
    mapRepositoryMap.remove(cacheId);
  }

  public EhcacheEntityResponse invoke(StateRepositoryOpMessage message) throws ClusterException {
    String cacheId = message.getCacheId();
    ServerStateRepository currentRepo = mapRepositoryMap.get(cacheId);
    if (currentRepo == null) {
      ServerStateRepository newRepo = new ServerStateRepository();
      currentRepo = mapRepositoryMap.putIfAbsent(cacheId, newRepo);
      if (currentRepo == null) {
        currentRepo = newRepo;
      }
    }
    return currentRepo.invoke(message);
  }

}
