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
package org.ehcache.clustered.server;

import java.util.UUID;

import org.ehcache.clustered.common.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage.ConfigureCacheManager;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage.ValidateCacheManager;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse;

import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.PassiveSynchronizationChannel;

public class EhcacheActiveEntity implements ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private final UUID identity;

  private ServerSideConfiguration configuration;


  EhcacheActiveEntity(byte[] config) {
    this.identity = ClusteredEhcacheIdentity.deserialize(config);
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {
    //nothing to do
  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
    //nothing to do
  }

  @Override
  public EhcacheEntityResponse invoke(ClientDescriptor clientDescriptor, EhcacheEntityMessage message) {
    switch (message.getType()) {
      case CONFIGURE: return configure((ConfigureCacheManager) message);
      case VALIDATE: return validate((ValidateCacheManager) message);
      default: throw new IllegalArgumentException("Unknown message " + message);
    }
  }

  @Override
  public void handleReconnect(ClientDescriptor clientDescriptor, byte[] extendedReconnectData) {
    //nothing to do
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel syncChannel, int concurrencyKey) {
    throw new UnsupportedOperationException("Active/passive is not supported yet");
  }

  @Override
  public void createNew() {
    //nothing to do
  }

  @Override
  public void loadExisting() {
    //nothing to do
  }

  @Override
  public void destroy() {
    //nothing to do
  }

  private EhcacheEntityResponse configure(ConfigureCacheManager message) throws IllegalStateException {
    if (configuration == null) {
      this.configuration = message.getConfiguration();
      return null;
    } else {
      throw new IllegalStateException("Clustered Cache Manager already configured");
    }
  }

  private EhcacheEntityResponse validate(ValidateCacheManager message)  throws IllegalArgumentException {
    if (Integer.bitCount(configuration.getMagic()) != Integer.bitCount(message.getConfiguration().getMagic())) {
      throw new IllegalArgumentException("Magic parameters not aligned");
    }
    return null;
  }
}
