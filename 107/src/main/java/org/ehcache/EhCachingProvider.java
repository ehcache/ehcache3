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

package org.ehcache;

import java.net.URI;
import java.util.Properties;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;

/**
 * @author Alex Snaps
 */
public class EhCachingProvider implements CachingProvider {

  @Override
  public CacheManager getCacheManager(final URI uri, final ClassLoader classLoader, final Properties properties) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public ClassLoader getDefaultClassLoader() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public URI getDefaultURI() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public Properties getDefaultProperties() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public CacheManager getCacheManager(final URI uri, final ClassLoader classLoader) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public CacheManager getCacheManager() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void close(final ClassLoader classLoader) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void close(final URI uri, final ClassLoader classLoader) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public boolean isSupported(final OptionalFeature optionalFeature) {
    switch(optionalFeature) {
      case STORE_BY_REFERENCE:
        return true;
      default:
        throw new IllegalArgumentException("Unknown OptionalFeature: " + optionalFeature.name());
    }
  }
}
