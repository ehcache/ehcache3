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

import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.util.Collection;
import java.util.Map;

/**
 * @author Alex Snaps
 */
public interface Configuration {

  /**
   * Mapping of names to {@link CacheConfiguration}, used to create the named {@link org.ehcache.Cache}
   * managed by a {@link org.ehcache.CacheManager}
   *
   * @return the map of names to configs
   */
  Map<String, CacheConfiguration<?, ?>> getCacheConfigurations();

  /**
   * All services initially used to bootstrap the {@link org.ehcache.CacheManager} and
   * its {@link org.ehcache.Cache} instances
   *
   * @return the collection of said configs
   */
  Collection<ServiceCreationConfiguration<?>> getServiceCreationConfigurations();

  /**
   * The {@link ClassLoader} to be associated with the {@link org.ehcache.CacheManager}
   *
   * @return The classloader
   */
  ClassLoader getClassLoader();
}
