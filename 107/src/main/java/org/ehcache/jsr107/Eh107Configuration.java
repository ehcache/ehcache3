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

import org.ehcache.config.Builder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;

import java.util.Collection;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;

import static org.ehcache.core.spi.service.ServiceUtils.findAmongst;

/**
 * Abstract {@link Configuration} implementation that enables building a JSR-107 compatible configuration from
 * a native Ehcache {@link CacheConfiguration}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class Eh107Configuration<K, V> implements Configuration<K, V> {

  private static final long serialVersionUID = 7324956960962454439L;

  /**
   * Creates a new JSR-107 {@link Configuration} from the provided {@link CacheConfiguration}.
   *
   * @param ehcacheConfig the native Ehcache configuration
   * @param <K> the key type
   * @param <V> the value type
   *
   * @return a JSR-107 configuration
   */
  public static <K, V> Configuration<K, V> fromEhcacheCacheConfiguration(CacheConfiguration<K, V> ehcacheConfig) {
    return new Eh107ConfigurationWrapper<>(ehcacheConfig);
  }

  /**
   * Creates a new JSR-107 {@link Configuration} from the provided {@link CacheConfiguration} obtained through a
   * {@link Builder}.
   *
   * @param ehcacheConfigBuilder the native Ehcache configuration through a builder
   * @param <K> the key type
   * @param <V> the value type
   *
   * @return a JSR-107 configuration
   */
  public static <K, V> Configuration<K, V> fromEhcacheCacheConfiguration(Builder<? extends CacheConfiguration<K, V>> ehcacheConfigBuilder) {
    return new Eh107ConfigurationWrapper<>(ehcacheConfigBuilder.build());
  }

  /**
   * Enables to unwrap the underlying configuration.
   *
   * @param clazz the unwrap target class
   * @param <T> the unwrap target type
   *
   * @return unwrapped type
   *
   * @throws IllegalArgumentException if the type cannot be unwrapped in the target type
   */
  public abstract <T> T unwrap(Class<T> clazz);

  abstract boolean isReadThrough();

  abstract boolean isWriteThrough();

  abstract boolean isStatisticsEnabled();

  abstract void setStatisticsEnabled(boolean enabled);

  abstract boolean isManagementEnabled();

  abstract void setManagementEnabled(boolean enabled);

  abstract void addCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration);

  abstract void removeCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration);

  static class Eh107ConfigurationWrapper<K, V> implements Configuration<K, V> {

    private static final long serialVersionUID = -142083549674760400L;

    private final transient CacheConfiguration<K, V> cacheConfiguration;

    private Eh107ConfigurationWrapper(CacheConfiguration<K, V> cacheConfiguration) {
      this.cacheConfiguration = cacheConfiguration;
    }

    CacheConfiguration<K, V> getCacheConfiguration() {
      return cacheConfiguration;
    }

    @Override
    public Class<K> getKeyType() {
      return cacheConfiguration.getKeyType();
    }

    @Override
    public Class<V> getValueType() {
      return cacheConfiguration.getValueType();
    }

    @Override
    public boolean isStoreByValue() {
      Collection<DefaultCopierConfiguration> copierConfig = findAmongst(DefaultCopierConfiguration.class, cacheConfiguration.getServiceConfigurations());
      return !copierConfig.isEmpty();
    }
  }
}
