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

import java.io.Serializable;
import java.util.UUID;

public abstract class StateRepositoryOpMessage extends EhcacheOperationMessage implements Serializable {

  private final String cacheId;
  private final String mapId;

  private UUID clientId;
  protected long id = NOT_REPLICATED;

  private StateRepositoryOpMessage(String cacheId, String mapId, UUID clientId) {
    this.cacheId = cacheId;
    this.mapId = mapId;
    this.clientId = clientId;
  }

  @Override
  public UUID getClientId() {
    if (clientId == null) {
      throw new AssertionError("Client Id cannot be null for StateRepository messages");
    }
    return this.clientId;
  }

  @Override
  public long getId() {
    return this.id;
  }

  @Override
  public void setId(long id) {
    this.id = id;
  }

  public String getCacheId() {
    return cacheId;
  }

  public String getMapId() {
    return mapId;
  }

  private static abstract class KeyBasedMessage extends StateRepositoryOpMessage {

    private final Object key;

    private KeyBasedMessage(final String cacheId, final String mapId, final Object key, final UUID clientId) {
      super(cacheId, mapId, clientId);
      this.key = key;
    }

    public Object getKey() {
      return key;
    }

  }

  public static class GetMessage extends KeyBasedMessage {

    public GetMessage(final String cacheId, final String mapId, final Object key, final UUID clientId) {
      super(cacheId, mapId, key, clientId);
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.GET_STATE_REPO;
    }
  }

  public static class PutIfAbsentMessage extends KeyBasedMessage {

    private final Object value;

    public PutIfAbsentMessage(final String cacheId, final String mapId, final Object key, final Object value, final UUID clientId) {
      super(cacheId, mapId, key, clientId);
      this.value = value;
    }

    public Object getValue() {
      return value;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.PUT_IF_ABSENT;
    }
  }

  public static class EntrySetMessage extends StateRepositoryOpMessage {

    public EntrySetMessage(final String cacheId, final String mapId, final UUID clientId) {
      super(cacheId, mapId, clientId);
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.ENTRY_SET;
    }
  }

}
