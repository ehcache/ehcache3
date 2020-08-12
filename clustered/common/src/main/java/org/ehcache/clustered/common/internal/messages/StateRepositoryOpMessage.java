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

public abstract class StateRepositoryOpMessage extends EhcacheOperationMessage implements Serializable {

  private static final long serialVersionUID = -6701802926010996981L;

  private final String cacheId;
  private final String mapId;

  private StateRepositoryOpMessage(String cacheId, String mapId) {
    this.cacheId = cacheId;
    this.mapId = mapId;
  }

  public String getCacheId() {
    return cacheId;
  }

  public String getMapId() {
    return mapId;
  }

  private static abstract class KeyBasedMessage extends StateRepositoryOpMessage {

    private static final long serialVersionUID = 2338704755924839309L;

    private final Object key;

    private KeyBasedMessage(final String cacheId, final String mapId, final Object key) {
      super(cacheId, mapId);
      this.key = key;
    }

    public Object getKey() {
      return key;
    }

  }

  public static class GetMessage extends KeyBasedMessage {

    private static final long serialVersionUID = 7263513962868446470L;

    public GetMessage(final String cacheId, final String mapId, final Object key) {
      super(cacheId, mapId, key);
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.GET_STATE_REPO;
    }
  }

  public static class PutIfAbsentMessage extends KeyBasedMessage {

    private static final long serialVersionUID = 2743653481411126124L;

    private final Object value;

    public PutIfAbsentMessage(final String cacheId, final String mapId, final Object key, final Object value) {
      super(cacheId, mapId, key);
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

    private static final long serialVersionUID = 5230634750732779978L;

    public EntrySetMessage(final String cacheId, final String mapId) {
      super(cacheId, mapId);
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.ENTRY_SET;
    }
  }

}
