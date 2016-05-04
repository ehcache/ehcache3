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

import java.io.Serializable;

import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.terracotta.entity.EntityMessage;

/**
 * Defines messages for interactions with an {@code EhcacheActiveEntity}.
 */
public abstract class EhcacheEntityMessage implements EntityMessage, Serializable {
  private static final long serialVersionUID = 223330390040183148L;

  public enum Type {
    CONFIGURE,
    VALIDATE,
    CREATE_SERVER_STORE,
    VALIDATE_SERVER_STORE,
    DESTROY_SERVER_STORE,
    DESTROY_ALL_SERVER_STORES
  }

  public abstract Type getType();

  public static EhcacheEntityMessage validate(ServerSideConfiguration config) {
    return new ValidateCacheManager(config);
  }

  public static class ValidateCacheManager extends EhcacheEntityMessage {
    private static final long serialVersionUID = 5742152283115139745L;

    private final ServerSideConfiguration configuration;

    private ValidateCacheManager(ServerSideConfiguration config) {
      this.configuration = config;
    }

    @Override
    public Type getType() {
      return Type.VALIDATE;
    }

    public ServerSideConfiguration getConfiguration() {
      return configuration;
    }
  }

  public static ConfigureCacheManager configure(ServerSideConfiguration config) {
    return new ConfigureCacheManager(config);
  }

  public static class ConfigureCacheManager extends EhcacheEntityMessage {
    private static final long serialVersionUID = 730771302294202898L;

    private final ServerSideConfiguration configuration;

    private ConfigureCacheManager(ServerSideConfiguration config) {
      this.configuration = config;
    }

    @Override
    public Type getType() {
      return Type.CONFIGURE;
    }

    public ServerSideConfiguration getConfiguration() {
      return configuration;
    }
  }

  public abstract static class BaseServerStore extends EhcacheEntityMessage {
    private static final long serialVersionUID = 4879477027919589726L;

    private final String name;
    private final ServerStoreConfiguration storeConfiguration;

    protected BaseServerStore(String name, ServerStoreConfiguration storeConfiguration) {
      this.name = name;
      this.storeConfiguration = storeConfiguration;
    }

    public String getName() {
      return name;
    }

    public ServerStoreConfiguration getStoreConfiguration() {
      return storeConfiguration;
    }

  }

  public static EhcacheEntityMessage createServerStore(String name, ServerStoreConfiguration serverStoreConfiguration) {
    return new CreateServerStore(name, serverStoreConfiguration);
  }

  /**
   * Message directing the <i>creation</i> of a new {@code ServerStore}.
   */
  public static class CreateServerStore extends BaseServerStore {
    private static final long serialVersionUID = -5832725455629624613L;

    private CreateServerStore(String name, ServerStoreConfiguration storeConfiguration) {
      super(name, storeConfiguration);
    }

    @Override
    public Type getType() {
      return Type.CREATE_SERVER_STORE;
    }
  }

  public static EhcacheEntityMessage validateServerStore(String name, ServerStoreConfiguration serverStoreConfiguration) {
    return new ValidateServerStore(name, serverStoreConfiguration);
  }

  /**
   * Message directing the <i>lookup</i> of a previously created {@code ServerStore}.
   */
  public static class ValidateServerStore extends BaseServerStore {
    private static final long serialVersionUID = 8762670006846832185L;

    private ValidateServerStore(String name, ServerStoreConfiguration storeConfiguration) {
      super(name, storeConfiguration);
    }

    @Override
    public Type getType() {
      return Type.VALIDATE_SERVER_STORE;
    }
  }

  public static EhcacheEntityMessage destroyServerStore(String name) {
    return new DestroyServerStore(name);
  }

  /**
   * Message directing the <i>destruction</i> of a {@code ServerStore}.
   */
  public static class DestroyServerStore extends EhcacheEntityMessage {
    private static final long serialVersionUID = -1772028546913171535L;

    private final String name;

    private DestroyServerStore(String name) {
      this.name = name;
    }

    @Override
    public Type getType() {
      return Type.DESTROY_SERVER_STORE;
    }

    public String getName() {
      return name;
    }
  }

  public static EhcacheEntityMessage destroyAllServerStores() {
    return new DestroyAllServerStores();
  }

  public static class DestroyAllServerStores extends EhcacheEntityMessage {
    private static final long serialVersionUID = 3050986754986404874L;

    private DestroyAllServerStores() {
    }

    @Override
    public Type getType() {
      return Type.DESTROY_ALL_SERVER_STORES;
    }
  }
}
