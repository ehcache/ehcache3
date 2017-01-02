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
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.PassiveSynchronizationChannel;

/**
 * ClusteredTierActiveEntity
 */
public class ClusteredTierActiveEntity implements ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  public ClusteredTierActiveEntity(ClusteredTierEntityConfiguration clusteredTierEntityConfiguration) {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public EhcacheEntityResponse invoke(ClientDescriptor clientDescriptor, EhcacheEntityMessage message) {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void loadExisting() {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void handleReconnect(ClientDescriptor clientDescriptor, byte[] extendedReconnectData) {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel, int concurrencyKey) {
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
