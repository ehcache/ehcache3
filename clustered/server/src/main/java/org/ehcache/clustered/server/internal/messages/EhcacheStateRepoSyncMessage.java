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

package org.ehcache.clustered.server.internal.messages;

import com.tc.classloader.CommonComponent;

import java.util.concurrent.ConcurrentMap;

/**
 * EhcacheStateRepoSyncMessage
 */
@CommonComponent
public class EhcacheStateRepoSyncMessage extends EhcacheSyncMessage {

  private final String cacheId;
  private final String mapId;
  private final ConcurrentMap<Object, Object> mappings;

  public EhcacheStateRepoSyncMessage(String cacheId, String mapId, ConcurrentMap<Object, Object> mappings) {
    this.cacheId = cacheId;
    this.mapId = mapId;
    this.mappings = mappings;
  }

  @Override
  public SyncMessageType getMessageType() {
    return SyncMessageType.STATE_REPO;
  }

  public ConcurrentMap<Object, Object> getMappings() {
    return mappings;
  }

  public String getCacheId() {
    return cacheId;
  }

  public String getMapId() {
    return mapId;
  }
}
