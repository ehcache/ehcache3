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

import org.ehcache.clustered.common.internal.store.Chain;

import com.tc.classloader.CommonComponent;

@CommonComponent
public class EntityDataSyncMessage extends EntitySyncMessage {

  private final String cacheId;
  private final long key;
  private final Chain chain;

  public EntityDataSyncMessage(final String cacheId, final long key, final Chain chain) {
    this.cacheId = cacheId;
    this.key = key;
    this.chain = chain;
  }

  @Override
  public SyncOp operation() {
    return SyncOp.DATA;
  }

  public String getCacheId() {
    return cacheId;
  }

  public long getKey() {
    return key;
  }

  public Chain getChain() {
    return chain;
  }
}
