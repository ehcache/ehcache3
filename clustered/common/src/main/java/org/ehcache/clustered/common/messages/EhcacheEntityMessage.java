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
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.terracotta.entity.EntityMessage;

/**
 *
 * @author cdennis
 */
public abstract class EhcacheEntityMessage implements EntityMessage, Serializable {

  public enum Type {
    CONFIGURE,
    VALIDATE;
  }

  public abstract Type getType();

  public static EhcacheEntityMessage validate(ServerSideConfiguration config) {
    return new ValidateCacheManager(config);
  }

  public static class ValidateCacheManager extends EhcacheEntityMessage {

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
}
