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

package org.ehcache.jsr107.config;

/**
 * ConfigurationElementState
 */
public enum ConfigurationElementState {

  /**
   * Indicates the configuration did not specify a value
   */
  UNSPECIFIED {
    @Override
    public boolean asBoolean() {
      throw new IllegalStateException("Cannot be converted to boolean");
    }
  },

  /**
   * Indicates the configuration disabled the option
   */
  DISABLED {
    @Override
    public boolean asBoolean() {
      return false;
    }
  },

  /**
   * Indicates the configuration enabled the option
   */
  ENABLED {
    @Override
    public boolean asBoolean() {
      return true;
    }
  };

  public abstract boolean asBoolean();
}
