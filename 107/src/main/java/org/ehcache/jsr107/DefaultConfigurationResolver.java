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
package org.ehcache.jsr107;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import javax.cache.CacheException;

class DefaultConfigurationResolver {

  static final String DEFAULT_CONFIG_PROPERTY_NAME = "ehcache.jsr107.config.default";

  static URI resolveConfigURI(Properties cacheManagerProperties) {
    Object config = null;

    config = cacheManagerProperties.get(DEFAULT_CONFIG_PROPERTY_NAME);

    if (config == null) {
      config = System.getProperties().get(DEFAULT_CONFIG_PROPERTY_NAME);
    }

    if (config == null) {
      return null;
    }

    if (config instanceof URI) {
      return (URI) config;
    }

    try {
      if (config instanceof URL) {
        return ((URL) config).toURI();
      }

      if (config instanceof String) {
        return new URI((String) config);
      }
    } catch (URISyntaxException use) {
      throw new CacheException(use);
    }

    throw new CacheException("Unsupported type for default config: " + config.getClass().getName());
  }

  private DefaultConfigurationResolver() {
    //
  }

}
