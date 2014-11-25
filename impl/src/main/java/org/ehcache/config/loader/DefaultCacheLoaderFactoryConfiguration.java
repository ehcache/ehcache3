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

package org.ehcache.config.loader;

import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.loader.CacheLoaderFactory;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Alex Snaps
 */
public class DefaultCacheLoaderFactoryConfiguration implements ServiceConfiguration<CacheLoaderFactory> {

  private Map<String, Class<? extends CacheLoader<?, ?>>> defaults = new HashMap<String, Class<? extends CacheLoader<?, ?>>>();

  @Override
  public Class<CacheLoaderFactory> getServiceType() {
    return CacheLoaderFactory.class;
  }

  public Map<String, Class<? extends CacheLoader<?, ?>>> getDefaults() {
    return defaults;
  }

  public DefaultCacheLoaderFactoryConfiguration addLoaderFor(String alias, final Class<? extends CacheLoader<?, ?>> clazz) {
    defaults.put(alias, clazz);
    return this;
  }
}
