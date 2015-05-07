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
package org.ehcache.config;

import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * Configuration of {@link org.ehcache.spi.serialization.SerializationProvider}
 *
 * @author Ludovic Orban
 */
public interface SerializationProviderConfiguration<T extends Service> extends ServiceConfiguration<T> {

  /**
   * Get the type of the serialization provider
   *
   * @return the type
   */
  Type getType();

  /**
   * Serialization provider types
   */
  enum Type {
    KEY,
    VALUE,
  }

}
