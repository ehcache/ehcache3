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

public abstract class LifecycleMessage extends EhcacheOperationMessage implements Serializable {

  public static class ValidateStoreManager extends LifecycleMessage {

    private final ServerSideConfiguration configuration;

    ValidateStoreManager(ServerSideConfiguration config) {
      this.configuration = config;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.VALIDATE;
    }

    public ServerSideConfiguration getConfiguration() {
      return configuration;
    }
  }

  /**
   * Message directing the <i>lookup</i> of a previously created {@code ServerStore}.
   */
  public static class ValidateServerStore extends LifecycleMessage {

    private final String name;
    private final ServerStoreConfiguration storeConfiguration;

    public ValidateServerStore(String name, ServerStoreConfiguration storeConfiguration) {
      this.name = name;
      this.storeConfiguration = storeConfiguration;
    }

    public String getName() {
      return name;
    }

    public ServerStoreConfiguration getStoreConfiguration() {
      return storeConfiguration;
    }

    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.VALIDATE_SERVER_STORE;
    }
  }

  public static class PrepareForDestroy extends LifecycleMessage {
    @Override
    public EhcacheMessageType getMessageType() {
      return EhcacheMessageType.PREPARE_FOR_DESTROY;
    }
  }
}
