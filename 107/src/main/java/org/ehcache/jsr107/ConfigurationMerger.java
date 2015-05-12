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
import org.ehcache.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.config.xml.XmlConfiguration;
import org.ehcache.internal.store.heap.service.OnHeapStoreServiceConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.slf4j.Logger;

import javax.cache.configuration.Configuration;

import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;

/**
 * ConfigurationMerger
 */
class ConfigurationMerger {

  private final XmlConfiguration xmlConfiguration;
  private final Jsr107Service jsr107Service;
  private final Eh107CacheLoaderWriterFactory cacheLoaderWriterFactory;
  private final Logger log;

  ConfigurationMerger(org.ehcache.config.Configuration ehConfig, Jsr107Service jsr107Service, Eh107CacheLoaderWriterFactory cacheLoaderWriterFactory, Logger log) {
    if (ehConfig instanceof XmlConfiguration) {
      xmlConfiguration = (XmlConfiguration) ehConfig;
    } else {
      xmlConfiguration = null;
    }
    this.jsr107Service = jsr107Service;
    this.cacheLoaderWriterFactory = cacheLoaderWriterFactory;
    this.log = log;
  }

  <K, V> ConfigHolder<K, V> mergeConfigurations(String cacheName, Configuration<K, V> configuration) {

    ConfigHolder<K, V> result = new ConfigHolder<K, V>();
    CacheConfigurationBuilder<K, V> builder = newCacheConfigurationBuilder();
    result.completeConfiguration = new Eh107CompleteConfiguration<K, V>(configuration);
    boolean useEhcacheExpiry = true;
    boolean useEhcacheLoaderWriter = true;

    try {

      result.cacheResources = new CacheResources<K, V>(cacheName, result.completeConfiguration);

      String templateName = getCacheTemplateName(cacheName);
      if (xmlConfiguration != null && templateName != null) {
        CacheConfigurationBuilder<K, V> templateBuilder = xmlConfiguration.newCacheConfigurationBuilderFromTemplate(templateName,
                        result.completeConfiguration.getKeyType(), result.completeConfiguration.getValueType());
        if (templateBuilder != null) {
          builder = templateBuilder;
          log.debug("Configuration of cache {} will be supplemented by template {}", cacheName, templateName);
        }
      }

      if (builder.hasDefaultExpiry()) {
        useEhcacheExpiry = false;
        builder = builder.withExpiry(result.cacheResources.getExpiryPolicy());
      } else {
        log.info("Cache {} will use expiry configuration from template {}", cacheName, templateName);
      }

      OnHeapStoreServiceConfiguration onHeapStoreServiceConfig = builder.getExistingServiceConfiguration(OnHeapStoreServiceConfiguration.class);
      if (onHeapStoreServiceConfig == null) {
        builder = builder.add(new OnHeapStoreServiceConfiguration().storeByValue(result.completeConfiguration.isStoreByValue()));
      }

      DefaultCacheLoaderWriterConfiguration loaderWriterConfiguration = builder.getExistingServiceConfiguration(DefaultCacheLoaderWriterConfiguration.class);
      if (loaderWriterConfiguration == null) {
        useEhcacheLoaderWriter = false;
        // No template loader/writer - let's activate the JSR-107 one if any
        if (result.completeConfiguration.isReadThrough() || result.completeConfiguration.isWriteThrough()) {
          CacheLoaderWriter<? super K, V> cacheLoaderWriter = result.cacheResources.getCacheLoaderWriter();
          if (cacheLoaderWriter != null) {
            cacheLoaderWriterFactory.registerJsr107Loader(cacheName, cacheLoaderWriter);
          }
        }
      } else {
        if (!result.completeConfiguration.isReadThrough() && result.completeConfiguration.isWriteThrough()) {
          log.warn("Activating Ehcache loader/writer for JSR-107 cache {} which was neither read-through nor write-through", cacheName);
        }
        log.info("Cache {} will use loader/writer configuration from template {}", cacheName, templateName);
      }

      result.cacheConfiguration = builder.buildConfig(result.completeConfiguration.getKeyType(), result.completeConfiguration
          .getValueType());

      Eh107Expiry<K, V> expiryPolicy;
      if (useEhcacheExpiry) {
        expiryPolicy = new EhcacheExpiryWrapper<K, V>(result.cacheConfiguration.getExpiry());
      } else {
        expiryPolicy = result.cacheResources.getExpiryPolicy();
      }

      result.cacheResources = new CacheResources<K, V>(cacheName, result.cacheResources.getCacheLoaderWriter(), expiryPolicy,
          result.cacheResources.getListenerResources());
      result.completeConfiguration = new Eh107CompleteConfiguration<K, V>(result.completeConfiguration, result.cacheConfiguration, useEhcacheExpiry, useEhcacheLoaderWriter);
      return result;
    } catch (Throwable throwable) {
      MultiCacheException mce = new MultiCacheException(throwable);
      if (result.cacheResources != null) {
        result.cacheResources.closeResources(mce);
      }
      throw mce;
    }
  }

  String getCacheTemplateName(String cacheName) {
    String template = jsr107Service.getTemplateNameForCache(cacheName);
    if (template != null) {
      return template;
    }
    return jsr107Service.getDefaultTemplate();
  }

  static class ConfigHolder<K, V> {
    CacheResources<K, V> cacheResources;
    CacheConfiguration<K, V> cacheConfiguration;
    Eh107CompleteConfiguration<K, V> completeConfiguration;
    boolean useEhcacheLoaderWriter;
  }
}
