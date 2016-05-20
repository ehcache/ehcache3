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
package org.ehcache.clustered.common.messages;

import java.nio.ByteBuffer;

import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.store.Chain;
import org.terracotta.entity.EntityMessage;

import static org.ehcache.clustered.common.messages.LifecycleMessage.CreateServerStore;
import static org.ehcache.clustered.common.messages.LifecycleMessage.ReleaseServerStore;
import static org.ehcache.clustered.common.messages.LifecycleMessage.ValidateServerStore;
import static org.ehcache.clustered.common.messages.LifecycleMessage.ValidateCacheManager;
import static org.ehcache.clustered.common.messages.LifecycleMessage.ConfigureCacheManager;
import static org.ehcache.clustered.common.messages.LifecycleMessage.DestroyServerStore;

/**
 * Defines messages for interactions with an {@code EhcacheActiveEntity}.
 */
public abstract class EhcacheEntityMessage implements EntityMessage {

  public enum Type {

    SERVER_STORE_OP((byte)0),
    LIFECYCLE_OP((byte)1);

    private final byte opCode;

    Type(byte opCode) {
      this.opCode = opCode;
    }

    public byte getOpCode() {
      return this.opCode;
    }
  }

  public abstract Type getType();

  public static EhcacheEntityMessage validate(ServerSideConfiguration config) {
    return new ValidateCacheManager(config);
  }

  public static EhcacheEntityMessage configure(ServerSideConfiguration config) {
    return new ConfigureCacheManager(config);
  }

  public static EhcacheEntityMessage createServerStore(String name, ServerStoreConfiguration serverStoreConfiguration) {
    return new CreateServerStore(name, serverStoreConfiguration);
  }

  public static EhcacheEntityMessage validateServerStore(String name, ServerStoreConfiguration serverStoreConfiguration) {
    return new ValidateServerStore(name, serverStoreConfiguration);
  }

  public static EhcacheEntityMessage releaseServerStore(String name) {
    return new ReleaseServerStore(name);
  }

  public static EhcacheEntityMessage destroyServerStore(String name) {
    return new DestroyServerStore(name);
  }

  public static EhcacheEntityMessage getOperation(String cacheId, long key) {
    return new ServerStoreOpMessage.GetMessage(cacheId, key);
  }

  public static EhcacheEntityMessage getAndAppendOperation(String cacheId, long key, ByteBuffer payload) {
    return new ServerStoreOpMessage.GetAndAppendMessage(cacheId, key, payload);
  }

  public static EhcacheEntityMessage appendOperation(String cacheId, long key, ByteBuffer payload) {
    return new ServerStoreOpMessage.AppendMessage(cacheId, key, payload);
  }

  public static EhcacheEntityMessage replaceAtHeadOperation(String cacheId, long key, Chain expect, Chain update) {
    return new ServerStoreOpMessage.ReplaceAtHeadMessage(cacheId, key, expect, update);
  }

  public static EhcacheEntityMessage clientInvalidateHashAck(String cacheId, long key) {
    return new ServerStoreOpMessage.ClientInvalidateHashAck(cacheId, key);
  }

}
