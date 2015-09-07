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
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.copy.CopierConfiguration;
import org.ehcache.config.copy.DefaultCopierConfiguration;
import org.ehcache.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.config.xml.XmlConfiguration;
import org.ehcache.internal.copy.IdentityCopier;
import org.ehcache.internal.copy.SerializingCopier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;

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
    final Eh107CompleteConfiguration<K, V> jsr107Configuration = new Eh107CompleteConfiguration<K, V>(configuration);

    Eh107Expiry<K, V> expiryPolicy = null;
    CacheLoaderWriter<? super K, V> loaderWriter = null;
    try {
      CacheConfigurationBuilder<K, V> builder = newCacheConfigurationBuilder();
      String templateName = jsr107Service.getTemplateNameForCache(cacheName);
      if (xmlConfiguration != null && templateName != null) {
        CacheConfigurationBuilder<K, V> templateBuilder = xmlConfiguration.newCacheConfigurationBuilderFromTemplate(templateName,
            jsr107Configuration.getKeyType(), jsr107Configuration.getValueType());
        if (templateBuilder != null) {
          builder = templateBuilder;
          LOG.info("Configuration of cache {} will be supplemented by template {}", cacheName, templateName);
        }
      }

      builder = handleStoreByValue(jsr107Configuration, builder);

      final boolean useJsr107Expiry = builder.hasDefaultExpiry();
      if (useJsr107Expiry) {
        expiryPolicy = initExpiryPolicy(jsr107Configuration);
        builder = builder.withExpiry(expiryPolicy);
      } else {
        LOG.info("Cache {} will use expiry configuration from template {}", cacheName, templateName);
      }

      boolean useEhcacheLoaderWriter;
      DefaultCacheLoaderWriterConfiguration ehcacheLoaderWriterConfiguration = builder.getExistingServiceConfiguration(DefaultCacheLoaderWriterConfiguration.class);
      if (ehcacheLoaderWriterConfiguration == null) {
        useEhcacheLoaderWriter = false;
        // No template loader/writer - let's activate the JSR-107 one if any
        loaderWriter = initCacheLoaderWriter(jsr107Configuration, new MultiCacheException());
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

      CacheConfiguration<K, V> cacheConfiguration = builder.buildConfig(jsr107Configuration.getKeyType(), jsr107Configuration.getValueType());

      if (!useJsr107Expiry) {
        expiryPolicy = new EhcacheExpiryWrapper<K, V>(cacheConfiguration.getExpiry());
      }

      return new ConfigHolder<K, V>(
          new CacheResources<K, V>(cacheName, loaderWriter, expiryPolicy, initCacheEventListeners(jsr107Configuration)),
          new Eh107CompleteConfiguration<K, V>(jsr107Configuration, cacheConfiguration, !useJsr107Expiry, useEhcacheLoaderWriter),
          cacheConfiguration,useEhcacheLoaderWriter);
    } catch (Throwable throwable) {
      MultiCacheException mce = new MultiCacheException(throwable);
      CacheResources.close(expiryPolicy, mce);
      CacheResources.close(loaderWriter, mce);
      throw mce;
    }
  }

  private <K, V> CacheConfigurationBuilder<K, V> handleStoreByValue(Eh107CompleteConfiguration<K, V> jsr107Configuration, CacheConfigurationBuilder<K, V> builder) {
    DefaultCopierConfiguration copierConfig = builder.getExistingServiceConfiguration(DefaultCopierConfiguration.class);
    LOG.info("No user configured copiers found. Falling back to JSR-107 defaults.");
    if(copierConfig == null) {
      if(jsr107Configuration.isStoreByValue()) {
        builder = builder.add(new DefaultCopierConfiguration<K>((Class)SerializingCopier.class, CopierConfiguration.Type.KEY))
            .add(new DefaultCopierConfiguration<K>((Class)SerializingCopier.class, CopierConfiguration.Type.VALUE));
        LOG.info("Using default SerializingCopier for JSR-107 store-by-value cache.");
      }
    }
    return builder;
  }

  private <K, V> Map<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>> initCacheEventListeners(CompleteConfiguration<K, V> config) {
    Map<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>> listenerResources = new ConcurrentHashMap<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>>();
    MultiCacheException mce = new MultiCacheException();
    for (CacheEntryListenerConfiguration<K, V> listenerConfig : config.getCacheEntryListenerConfigurations()) {
      listenerResources.put(listenerConfig, ListenerResources.createListenerResources(listenerConfig, mce));
    }
    return listenerResources;
  }

  private <K, V> Eh107Expiry<K, V> initExpiryPolicy(CompleteConfiguration<K, V> config) {
    return new ExpiryPolicyToEhcacheExpiry<K, V>(config.getExpiryPolicyFactory().create());
  }

  private <K, V> CacheLoaderWriter<K, V> initCacheLoaderWriter(CompleteConfiguration<K, V> config, MultiCacheException mce) {
    Factory<CacheLoader<K, V>> cacheLoaderFactory = config.getCacheLoaderFactory();
    Factory<CacheWriter<K, V>> cacheWriterFactory = getCacheWriterFactory(config);

    CacheLoader<K, V> cacheLoader = cacheLoaderFactory == null ? null : cacheLoaderFactory.create();
    CacheWriter<K, V> cacheWriter;
    try {
      cacheWriter = cacheWriterFactory == null ? null : cacheWriterFactory.create();
    } catch (Throwable t) {
      if (t != mce) {
        mce.addThrowable(t);
      }
      CacheResources.close(cacheLoader, mce);
      throw mce;
    }

    if (cacheLoader == null && cacheWriter == null) {
      return null;
    } else {
      return new Eh107CacheLoaderWriter<K, V>(cacheLoader, cacheWriter);
    }
  }

  @SuppressWarnings("unchecked")
  private static <K, V> Factory<CacheWriter<K, V>> getCacheWriterFactory(CompleteConfiguration<K, V> config) {
    // I could be wrong, but I don't think this factory should be typed the way it is. The factory
    // should be parameterized with (K, V) and it's methods take <? extend K>, etc
    Object factory = config.getCacheWriterFactory();
    return (Factory<javax.cache.integration.CacheWriter<K, V>>) factory;
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
