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

import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;

/**
 * Represents the minimal read-only configuration for a Cache to be, or an already existing one
 *
 * @author Alex Snaps
 */
public interface CacheConfiguration<K, V> {

  /**
   * Not sure whether this should be exposed on this interface really
   * @return unmodifiable collection of service configuration related to this cache
   */
  Collection<ServiceConfiguration<?>> getServiceConfigurations();

  /**
   * The type of the key for this cache
   * @return a non null value, where Object.class is the widest type
   */
  Class<K> getKeyType();

  /**
   * The type of the value held in this cache
   * @return a non null value, where Object.class is the widest type
   */
  Class<V> getValueType();
}
