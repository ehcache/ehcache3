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

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;

import java.io.Serializable;

public abstract class LifecycleMessage extends EhcacheEntityMessage implements Serializable {

  public enum LifeCycleOp {
    CONFIGURE,
    VALIDATE,
    CREATE_SERVER_STORE,
    VALIDATE_SERVER_STORE,
    RELEASE_SERVER_STORE,
    DESTROY_SERVER_STORE,
  }

  @Override
  public byte getOpCode() {
    return getType().getCode();
  }

  @Override
  public Type getType() {
    return Type.LIFECYCLE_OP;
  }

  public abstract LifeCycleOp operation();

  @Override
  public String toString() {
    return getType() + "#" + operation();
  }

  public static class ValidateStoreManager extends LifecycleMessage {
    private static final long serialVersionUID = 5742152283115139745L;

    private final ServerSideConfiguration configuration;

    ValidateStoreManager(ServerSideConfiguration config) {
      this.configuration = config;
    }

    @Override
    public LifeCycleOp operation() {
      return LifeCycleOp.VALIDATE;
    }

    public ServerSideConfiguration getConfiguration() {
      return configuration;
    }
  }

  public static class ConfigureStoreManager extends LifecycleMessage {
    private static final long serialVersionUID = 730771302294202898L;

    private final ServerSideConfiguration configuration;

    ConfigureStoreManager(ServerSideConfiguration config) {
      this.configuration = config;
    }

    @Override
    public LifeCycleOp operation() {
      return LifeCycleOp.CONFIGURE;
    }

    public ServerSideConfiguration getConfiguration() {
      return configuration;
    }
  }

  public abstract static class BaseServerStore extends LifecycleMessage {
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

  /**
   * Message directing the <i>creation</i> of a new {@code ServerStore}.
   */
  public static class CreateServerStore extends BaseServerStore {
    private static final long serialVersionUID = -5832725455629624613L;

    CreateServerStore(String name, ServerStoreConfiguration storeConfiguration) {
      super(name, storeConfiguration);
    }

    @Override
    public LifeCycleOp operation() {
      return LifeCycleOp.CREATE_SERVER_STORE;
    }
  }

  /**
   * Message directing the <i>lookup</i> of a previously created {@code ServerStore}.
   */
  public static class ValidateServerStore extends BaseServerStore {
    private static final long serialVersionUID = 8762670006846832185L;

    ValidateServerStore(String name, ServerStoreConfiguration storeConfiguration) {
      super(name, storeConfiguration);
    }

    @Override
    public LifeCycleOp operation() {
      return LifeCycleOp.VALIDATE_SERVER_STORE;
    }
  }

  /**
   * Message disconnecting a client from a {@code ServerStore}.
   */
  public static class ReleaseServerStore extends LifecycleMessage {
    private static final long serialVersionUID = 6486779694089287953L;

    private final String name;

    ReleaseServerStore(String name) {
      this.name = name;
    }

    @Override
    public LifeCycleOp operation() {
      return LifeCycleOp.RELEASE_SERVER_STORE;
    }

    public String getName() {
      return name;
    }
  }

  /**
   * Message directing the <i>destruction</i> of a {@code ServerStore}.
   */
  public static class DestroyServerStore extends LifecycleMessage {
    private static final long serialVersionUID = -1772028546913171535L;

    private final String name;

    DestroyServerStore(String name) {
      this.name = name;
    }

    @Override
    public LifeCycleOp operation() {
      return LifeCycleOp.DESTROY_SERVER_STORE;
    }

    public String getName() {
      return name;
    }
  }
}
