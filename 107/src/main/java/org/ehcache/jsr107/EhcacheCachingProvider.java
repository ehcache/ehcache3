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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;

import org.ehcache.EhcacheManager;
import org.ehcache.config.Configuration;
import org.ehcache.config.DefaultConfiguration;
import org.ehcache.config.Jsr107Configuration;
import org.ehcache.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.config.xml.XmlConfiguration;
import org.ehcache.management.ManagementRegistry;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.alias.AliasService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.util.ClassLoading;

/**
 * @author teck
 */
public class EhcacheCachingProvider implements CachingProvider {

  private static final String DEFAULT_URI_STRING = "urn:X-ehcache:jsr107-default-config";

  private static final URI URI_DEFAULT;

  private final Map<ClassLoader, ConcurrentMap<URI, Eh107CacheManager>> cacheManagers = new WeakHashMap<ClassLoader, ConcurrentMap<URI, Eh107CacheManager>>();

  static {
    try {
      URI_DEFAULT = new URI(DEFAULT_URI_STRING);
    } catch (URISyntaxException e) {
      throw new javax.cache.CacheException(e);
    }
  }

  @Override
  public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
    uri = uri == null ? getDefaultURI() : uri;
    classLoader = classLoader == null ? getDefaultClassLoader() : classLoader;
    properties = properties == null ? new Properties() : cloneProperties(properties);

    if (URI_DEFAULT.equals(uri)) {
      URI override = DefaultConfigurationResolver.resolveConfigURI(properties);
      if (override != null) {
        uri = override;
      }
    }

    Eh107CacheManager cacheManager;
    ConcurrentMap<URI, Eh107CacheManager> byURI;

    synchronized (cacheManagers) {
      byURI = cacheManagers.get(classLoader);
      if (byURI == null) {
        byURI = new ConcurrentHashMap<URI, Eh107CacheManager>();
        cacheManagers.put(classLoader, byURI);
      }

      cacheManager = byURI.get(uri);
      if (cacheManager == null || cacheManager.isClosed()) {

        if(cacheManager != null) {
          byURI.remove(uri, cacheManager);
        }

        Configuration config;
        try {
          if (URI_DEFAULT.equals(uri)) {
            config = new DefaultConfiguration(classLoader);
          } else {
            config = new XmlConfiguration(uri.toURL(), classLoader);
          }
        } catch (Exception e) {
          throw new javax.cache.CacheException(e);
        }

        Eh107CacheLoaderWriterProvider cacheLoaderWriterFactory = new Eh107CacheLoaderWriterProvider();
        Jsr107Service jsr107Service = new DefaultJsr107Service(ServiceLocator.findSingletonAmongst(Jsr107Configuration.class, config.getServiceCreationConfigurations().toArray()));
        ServiceCollector serviceCollector = new ServiceCollector();

        Collection<Service> services = new ArrayList<Service>();
        services.add(cacheLoaderWriterFactory);
        services.add(jsr107Service);
        if(ServiceLocator.findSingletonAmongst(DefaultSerializationProviderConfiguration.class, config.getServiceCreationConfigurations().toArray()) == null) {
          services.add(new DefaultJsr107SerializationProvider());
        }
        services.add(serviceCollector);
       
        EhcacheManager ehcacheManager = new EhcacheManager(config, services, !jsr107Service.jsr107CompliantAtomics());
        ehcacheManager.init();
        cacheManager = new Eh107CacheManager(this, ehcacheManager, properties, classLoader, uri,
            serviceCollector.managementRegistry, serviceCollector.aliasService, new ConfigurationMerger(config, jsr107Service, cacheLoaderWriterFactory));
        byURI.put(uri, cacheManager);
      }
    }

    return cacheManager;
  }

  @ServiceDependencies({ManagementRegistry.class, AliasService.class})
  static class ServiceCollector implements Service {

    public volatile ManagementRegistry managementRegistry;
    public volatile AliasService aliasService;

    @Override
    public void start(ServiceProvider serviceProvider) {
      managementRegistry = serviceProvider.getService(ManagementRegistry.class);
      aliasService = serviceProvider.getService(AliasService.class);
    }

    @Override
    public void stop() {
      managementRegistry = null;
      aliasService = null;
    }

  }

  @Override
  public ClassLoader getDefaultClassLoader() {
    return ClassLoading.getDefaultClassLoader();
  }

  @Override
  public URI getDefaultURI() {
    return URI_DEFAULT;
  }

  @Override
  public Properties getDefaultProperties() {
    return new Properties();
  }

  @Override
  public CacheManager getCacheManager(final URI uri, final ClassLoader classLoader) {
    return getCacheManager(uri, classLoader, null);
  }

  @Override
  public CacheManager getCacheManager() {
    return getCacheManager(getDefaultURI(), getDefaultClassLoader());
  }

  @Override
  public void close() {
    synchronized (cacheManagers) {
      for (Map.Entry<ClassLoader, ConcurrentMap<URI, Eh107CacheManager>> entry : cacheManagers.entrySet()) {
        for (Eh107CacheManager cacheManager : entry.getValue().values()) {
          cacheManager.close();
        }
      }
      cacheManagers.clear();
    }
  }

  @Override
  public void close(final ClassLoader classLoader) {
    if (classLoader == null) {
      throw new NullPointerException();
    }

    MultiCacheException closeException = new MultiCacheException();
    synchronized (cacheManagers) {
      final ConcurrentMap<URI, Eh107CacheManager> map = cacheManagers.remove(classLoader);
      if (map != null) {
        for (Eh107CacheManager cacheManager : map.values()) {
          cacheManager.closeInternal(closeException);
        }
      }
    }
    
    closeException.throwIfNotEmpty();
  }

  @Override
  public void close(final URI uri, final ClassLoader classLoader) {
    if (uri == null || classLoader == null) {
      throw new NullPointerException();
    }

    MultiCacheException closeException = new MultiCacheException();
    synchronized (cacheManagers) {
      final ConcurrentMap<URI, Eh107CacheManager> map = cacheManagers.get(classLoader);
      if (map != null) {
        final Eh107CacheManager cacheManager = map.remove(uri);
        if (cacheManager != null) {
          cacheManager.closeInternal(closeException);
        }
      }
    }
    closeException.throwIfNotEmpty();
  }

  @Override
  public boolean isSupported(final OptionalFeature optionalFeature) {
    if (optionalFeature == null) {
      throw new NullPointerException();
    }

    // this switch statement written w/o "default:" to let use know if a new
    // optional feature is added in the spec
    switch (optionalFeature) {
    case STORE_BY_REFERENCE:
      return true;
    }

    throw new IllegalArgumentException("Unknown OptionalFeature: " + optionalFeature.name());
  }

  void close(Eh107CacheManager cacheManager, MultiCacheException closeException) {
    try {
      synchronized (cacheManagers) {
        final ConcurrentMap<URI, Eh107CacheManager> map = cacheManagers.get(cacheManager.getClassLoader());
        if (map != null && map.remove(cacheManager.getURI()) != null) {
          cacheManager.closeInternal(closeException);
        }
      }
    } catch (Throwable t) {
      closeException.addThrowable(t);
    }
  }
  
  private static Properties cloneProperties(Properties properties) {
    Properties clone = new Properties();
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      clone.put(entry.getKey(), entry.getValue());
    }
    return clone;
  }
  
}
