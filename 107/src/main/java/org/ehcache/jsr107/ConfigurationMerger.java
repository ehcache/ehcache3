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

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.core.InternalCache;
import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.jsr107.config.ConfigurationElementState;
import org.ehcache.jsr107.config.Jsr107CacheConfiguration;
import org.ehcache.jsr107.internal.Jsr107CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;
import static org.ehcache.jsr107.CloseUtil.closeAllAfter;

/**
 * ConfigurationMerger
 */
class ConfigurationMerger {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationMerger.class);

  private final XmlConfiguration xmlConfiguration;
  private final Jsr107Service jsr107Service;
  private final Eh107CacheLoaderWriterProvider cacheLoaderWriterFactory;

  ConfigurationMerger(org.ehcache.config.Configuration ehConfig, Jsr107Service jsr107Service, Eh107CacheLoaderWriterProvider cacheLoaderWriterFactory) {
    if (ehConfig instanceof XmlConfiguration) {
      xmlConfiguration = (XmlConfiguration) ehConfig;
    } else {
      xmlConfiguration = null;
    }
    this.jsr107Service = jsr107Service;
    this.cacheLoaderWriterFactory = cacheLoaderWriterFactory;
  }

  <K, V> ConfigHolder<K, V> mergeConfigurations(String cacheName, Configuration<K, V> configuration) {
    final Eh107CompleteConfiguration<K, V> jsr107Configuration = new Eh107CompleteConfiguration<>(configuration);

    Eh107Expiry<K, V> expiryPolicy = null;
    Jsr107CacheLoaderWriter<? super K, V> loaderWriter = null;
    try {
      CacheConfigurationBuilder<K, V> builder = newCacheConfigurationBuilder(configuration.getKeyType(), configuration.getValueType(), heap(Long.MAX_VALUE));

      String templateName = jsr107Service.getTemplateNameForCache(cacheName);
      if (xmlConfiguration != null && templateName != null) {
        CacheConfigurationBuilder<K, V> templateBuilder;
        try {
          templateBuilder = xmlConfiguration.newCacheConfigurationBuilderFromTemplate(templateName,
              jsr107Configuration.getKeyType(), jsr107Configuration.getValueType());
        } catch (IllegalStateException e) {
          templateBuilder = xmlConfiguration.newCacheConfigurationBuilderFromTemplate(templateName,
                        jsr107Configuration.getKeyType(), jsr107Configuration.getValueType(), heap(Long.MAX_VALUE));
        }
        if (templateBuilder != null) {
          builder = templateBuilder;
          LOG.info("Configuration of cache {} will be supplemented by template {}", cacheName, templateName);
        }
      }

      builder = handleStoreByValue(jsr107Configuration, builder, cacheName);

      final boolean hasConfiguredExpiry = builder.hasConfiguredExpiry();
      if (hasConfiguredExpiry) {
        LOG.info("Cache {} will use expiry configuration from template {}", cacheName, templateName);
      } else {
        expiryPolicy = initExpiryPolicy(jsr107Configuration);
        builder = builder.withExpiry(expiryPolicy);
      }

      boolean useEhcacheLoaderWriter;
      CacheLoaderWriterConfiguration<?> ehcacheLoaderWriterConfiguration = builder.getService(DefaultCacheLoaderWriterConfiguration.class);
      if (ehcacheLoaderWriterConfiguration == null) {
        useEhcacheLoaderWriter = false;
        // No template loader/writer - let's activate the JSR-107 one if any
        loaderWriter = initCacheLoaderWriter(jsr107Configuration);
        if (loaderWriter != null && (jsr107Configuration.isReadThrough() || jsr107Configuration.isWriteThrough())) {
          cacheLoaderWriterFactory.registerJsr107Loader(cacheName, loaderWriter);
        }
      } else {
        useEhcacheLoaderWriter = true;
        if (!jsr107Configuration.isReadThrough() && !jsr107Configuration.isWriteThrough()) {
          LOG.warn("Activating Ehcache loader/writer for JSR-107 cache {} which was neither read-through nor write-through", cacheName);
        }
        LOG.info("Cache {} will use loader/writer configuration from template {}", cacheName, templateName);
      }

      CacheConfiguration<K, V> cacheConfiguration = builder.build();

      setupManagementAndStatsInternal(jsr107Configuration, findSingletonAmongst(Jsr107CacheConfiguration.class, cacheConfiguration.getServiceConfigurations()));

      if (hasConfiguredExpiry) {
        expiryPolicy = new EhcacheExpiryWrapper<>(cacheConfiguration.getExpiryPolicy());
      }

      return new ConfigHolder<>(
        new CacheResources<>(cacheName, loaderWriter, expiryPolicy, initCacheEventListeners(jsr107Configuration)),
        new Eh107CompleteConfiguration<>(jsr107Configuration, cacheConfiguration, hasConfiguredExpiry, useEhcacheLoaderWriter),
        cacheConfiguration, useEhcacheLoaderWriter);
    } catch (Throwable throwable) {
      if (throwable instanceof IllegalArgumentException) {
        throw closeAllAfter((IllegalArgumentException) throwable, expiryPolicy, loaderWriter);
      } else {
        throw closeAllAfter(new CacheException(throwable), expiryPolicy, loaderWriter);
      }
    }
  }

  private <K, V> CacheConfigurationBuilder<K, V> handleStoreByValue(Eh107CompleteConfiguration<K, V> jsr107Configuration, CacheConfigurationBuilder<K, V> builder, String cacheName) {
    @SuppressWarnings("unchecked")
    Collection<DefaultCopierConfiguration<?>> copierConfigs = builder.getServices((Class<DefaultCopierConfiguration<?>>) (Class) DefaultCopierConfiguration.class);
    if(copierConfigs.isEmpty()) {
      if(jsr107Configuration.isStoreByValue()) {
        if (xmlConfiguration != null) {
          DefaultCopyProviderConfiguration defaultCopyProviderConfiguration = findSingletonAmongst(DefaultCopyProviderConfiguration.class,
              xmlConfiguration.getServiceCreationConfigurations());
          if (defaultCopyProviderConfiguration != null) {
            Map<Class<?>, DefaultCopierConfiguration<?>> defaults = defaultCopyProviderConfiguration.getDefaults();
            handleCopierDefaultsforImmutableTypes(defaults);
            boolean matchingDefault = false;
            if (defaults.containsKey(jsr107Configuration.getKeyType())) {
              matchingDefault = true;
            } else {
              builder = builder.withService(new DefaultCopierConfiguration<>(SerializingCopier.<K>asCopierClass(), DefaultCopierConfiguration.Type.KEY));
            }
            if (defaults.containsKey(jsr107Configuration.getValueType())) {
              matchingDefault = true;
            } else {
              builder = builder.withService(new DefaultCopierConfiguration<>(SerializingCopier.<K>asCopierClass(), DefaultCopierConfiguration.Type.VALUE));
            }
            if (matchingDefault) {
              LOG.info("CacheManager level copier configuration overwriting JSR-107 by-value semantics for cache {}", cacheName);
            }
            return builder;
          }
        }
        builder = addDefaultCopiers(builder, jsr107Configuration.getKeyType(), jsr107Configuration.getValueType());
        LOG.debug("Using default Copier for JSR-107 store-by-value cache {}", cacheName);
      }
    } else {
      LOG.info("Cache level copier configuration overwriting JSR-107 by-value semantics for cache {}", cacheName);
    }
    return builder;
  }

  @SuppressWarnings("unchecked")
  private static <K, V> CacheConfigurationBuilder<K, V> addDefaultCopiers(CacheConfigurationBuilder<K, V> builder, Class<K> keyType, Class<V> valueType ) {
    Set<Class<?>> immutableTypes = new HashSet<>();
    immutableTypes.add(String.class);
    immutableTypes.add(Long.class);
    immutableTypes.add(Float.class);
    immutableTypes.add(Double.class);
    immutableTypes.add(Character.class);
    immutableTypes.add(Integer.class);
    if (immutableTypes.contains(keyType)) {
      builder = builder.withService(new DefaultCopierConfiguration<K>((Class)Eh107IdentityCopier.class, DefaultCopierConfiguration.Type.KEY));
    } else {
      builder = builder.withService(new DefaultCopierConfiguration<>(SerializingCopier.<K>asCopierClass(), DefaultCopierConfiguration.Type.KEY));
    }

    if (immutableTypes.contains(valueType)) {
      builder = builder.withService(new DefaultCopierConfiguration<K>((Class)Eh107IdentityCopier.class, DefaultCopierConfiguration.Type.VALUE));
    } else {
      builder = builder.withService(new DefaultCopierConfiguration<>(SerializingCopier.<K>asCopierClass(), DefaultCopierConfiguration.Type.VALUE));
    }
    return builder;
  }

  private static void handleCopierDefaultsforImmutableTypes(Map<Class<?>, DefaultCopierConfiguration<?>> defaults) {
    addIdentityCopierIfNoneRegistered(defaults, Long.class);
    addIdentityCopierIfNoneRegistered(defaults, Integer.class);
    addIdentityCopierIfNoneRegistered(defaults, String.class);
    addIdentityCopierIfNoneRegistered(defaults, Float.class);
    addIdentityCopierIfNoneRegistered(defaults, Double.class);
    addIdentityCopierIfNoneRegistered(defaults, Character.class);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void addIdentityCopierIfNoneRegistered(Map<Class<?>, DefaultCopierConfiguration<?>> defaults, Class<?> clazz) {
    if (!defaults.containsKey(clazz)) {
      defaults.put(clazz, new DefaultCopierConfiguration(Eh107IdentityCopier.class, DefaultCopierConfiguration.Type.VALUE));
    }
  }

  private <K, V> Map<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>> initCacheEventListeners(CompleteConfiguration<K, V> config) {
    Map<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>> listenerResources = new ConcurrentHashMap<>();
    for (CacheEntryListenerConfiguration<K, V> listenerConfig : config.getCacheEntryListenerConfigurations()) {
      listenerResources.put(listenerConfig, ListenerResources.createListenerResources(listenerConfig));
    }
    return listenerResources;
  }

  private <K, V> Eh107Expiry<K, V> initExpiryPolicy(CompleteConfiguration<K, V> config) {
    return new ExpiryPolicyToEhcacheExpiry<>(config.getExpiryPolicyFactory().create());
  }

  private <K, V> Jsr107CacheLoaderWriter<K, V> initCacheLoaderWriter(CompleteConfiguration<K, V> config) {
    Factory<CacheLoader<K, V>> cacheLoaderFactory = config.getCacheLoaderFactory();
    @SuppressWarnings("unchecked")
    Factory<CacheWriter<K, V>> cacheWriterFactory = (Factory<CacheWriter<K, V>>) (Object) config.getCacheWriterFactory();

    if (config.isReadThrough() && cacheLoaderFactory == null) {
      throw new IllegalArgumentException("read-through enabled without a CacheLoader factory provided");
    }
    if (config.isWriteThrough() && cacheWriterFactory == null) {
      throw new IllegalArgumentException("write-through enabled without a CacheWriter factory provided");
    }

    CacheLoader<K, V> cacheLoader = cacheLoaderFactory == null ? null : cacheLoaderFactory.create();
    CacheWriter<K, V> cacheWriter;
    try {
      cacheWriter = cacheWriterFactory == null ? null : cacheWriterFactory.create();
    } catch (Throwable t) {
      throw closeAllAfter(new CacheException(t), cacheLoader);
    }

    if (cacheLoader == null && cacheWriter == null) {
      return null;
    } else {
      return new Eh107CacheLoaderWriter<>(cacheLoader, config.isReadThrough(), cacheWriter, config.isWriteThrough());
    }
  }

  void setUpManagementAndStats(InternalCache<?, ?> cache, Eh107Configuration<?, ?> configuration) {
    Jsr107CacheConfiguration cacheConfiguration = ServiceUtils.findSingletonAmongst(Jsr107CacheConfiguration.class, cache
        .getRuntimeConfiguration().getServiceConfigurations());
    setupManagementAndStatsInternal(configuration, cacheConfiguration);
  }

  private void setupManagementAndStatsInternal(Eh107Configuration<?, ?> configuration, Jsr107CacheConfiguration cacheConfiguration) {
    ConfigurationElementState enableManagement = jsr107Service.isManagementEnabledOnAllCaches();
    ConfigurationElementState enableStatistics = jsr107Service.isStatisticsEnabledOnAllCaches();
    if (cacheConfiguration != null) {
      ConfigurationElementState managementEnabled = cacheConfiguration.isManagementEnabled();
      if (managementEnabled != null && managementEnabled != ConfigurationElementState.UNSPECIFIED) {
        enableManagement = managementEnabled;
      }
      ConfigurationElementState statisticsEnabled = cacheConfiguration.isStatisticsEnabled();
      if (statisticsEnabled != null && statisticsEnabled != ConfigurationElementState.UNSPECIFIED) {
        enableStatistics = statisticsEnabled;
      }
    }
    if (enableManagement != null && enableManagement != ConfigurationElementState.UNSPECIFIED) {
      configuration.setManagementEnabled(enableManagement.asBoolean());
    }
    if (enableStatistics != null && enableStatistics != ConfigurationElementState.UNSPECIFIED) {
      configuration.setStatisticsEnabled(enableStatistics.asBoolean());
    }
  }

  static class ConfigHolder<K, V> {
    final CacheResources<K, V> cacheResources;
    final CacheConfiguration<K, V> cacheConfiguration;
    final Eh107CompleteConfiguration<K, V> jsr107Configuration;
    final boolean useEhcacheLoaderWriter;

    public ConfigHolder(CacheResources<K, V> cacheResources, Eh107CompleteConfiguration<K, V> jsr107Configuration, CacheConfiguration<K, V> cacheConfiguration, boolean useEhcacheLoaderWriter) {
      this.cacheResources = cacheResources;
      this.jsr107Configuration = jsr107Configuration;
      this.cacheConfiguration = cacheConfiguration;
      this.useEhcacheLoaderWriter = useEhcacheLoaderWriter;
    }
  }
}
