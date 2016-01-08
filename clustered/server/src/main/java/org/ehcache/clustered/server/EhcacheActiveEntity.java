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

import java.util.Arrays;
import java.util.UUID;

import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.PassiveSynchronizationChannel;

import static org.ehcache.clustered.server.ConcurrencyStrategies.noConcurrency;

public class EhcacheActiveEntity implements ActiveServerEntity<EntityMessage, EntityResponse> {

  private final byte[] config;
  private final UUID identity;
  
  EhcacheActiveEntity(byte[] config) {
    if (config.length != 16) {
      throw new IllegalArgumentException("Expected a 16 byte (UUID) config stream");
    }
    this.config = Arrays.copyOf(config, 16);
    long msl = getLong(this.config, 0);
    long lsl = getLong(this.config, 8);
    this.identity = new UUID(msl, lsl);
  }

  @Override
  public ConcurrencyStrategy<EntityMessage> getConcurrencyStrategy() {
    return noConcurrency();
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
  public byte[] getConfig() {
    return Arrays.copyOf(config, 16);
  }

  @Override
  public EntityResponse invoke(ClientDescriptor clientDescriptor, EntityMessage message) {
    throw new UnsupportedOperationException("No messages supported yet");
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
  public MessageCodec<EntityMessage, EntityResponse> getMessageCodec() {
    return null;
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

  private static long getLong(byte[] array, int offset) {
    if (array.length < offset + 8) {
      throw new ArrayIndexOutOfBoundsException();
    } else {
      long result = 0;
      for (int i = 0; i < 8; i++) {
        result <<= 8;
        result |= array[offset + i] & 0xff;
      }
      return result;
    }
  }
}
