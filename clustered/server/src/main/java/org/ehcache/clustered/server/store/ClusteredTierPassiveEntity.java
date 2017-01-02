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

package org.ehcache.clustered.server.store;

import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.store.ClusteredTierEntityConfiguration;
import org.terracotta.entity.PassiveServerEntity;

/**
 * ClusteredTierPassiveEntity
 */
public class ClusteredTierPassiveEntity implements PassiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  public ClusteredTierPassiveEntity(ClusteredTierEntityConfiguration clusteredTierEntityConfiguration) {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void invoke(EhcacheEntityMessage message) {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void startSyncEntity() {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void endSyncEntity() {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void startSyncConcurrencyKey(int concurrencyKey) {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void endSyncConcurrencyKey(int concurrencyKey) {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void createNew() {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void destroy() {
    throw new UnsupportedOperationException("TODO Implement me!");
  }
}
