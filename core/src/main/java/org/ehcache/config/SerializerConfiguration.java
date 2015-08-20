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

import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.service.ServiceUseConfiguration;

/**
 * Configuration passed to {@link org.ehcache.spi.serialization.SerializationProvider} in order to control
 * the creation of a {@link org.ehcache.spi.serialization.Serializer}.
 *
 * @author Ludovic Orban
 */
public interface SerializerConfiguration<T extends SerializationProvider> extends ServiceUseConfiguration<T> {

  /**
   * Get the type of the serializer configured
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
