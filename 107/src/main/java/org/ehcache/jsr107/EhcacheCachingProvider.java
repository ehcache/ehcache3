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

import org.ehcache.config.Configuration;
import org.ehcache.core.config.DefaultConfiguration;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.impl.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.jsr107.config.Jsr107Configuration;
import org.ehcache.jsr107.config.Jsr107Service;
import org.ehcache.jsr107.internal.DefaultJsr107Service;
import org.ehcache.spi.service.Service;
import org.ehcache.xml.XmlConfiguration;

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

/**
 * {@link CachingProvider} implementation for Ehcache.
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

  /**
   * {@inheritDoc}
   */
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

    return getCacheManager(new ConfigSupplier(uri, classLoader), properties);
  }

  /**
   * Enables to create a JSR-107 {@link CacheManager} based on the provided Ehcache {@link Configuration}.
   *
   * @param uri the URI identifying this cache manager
   * @param config the Ehcache configuration to use
   *
   * @return a cache manager
   */
  public CacheManager getCacheManager(URI uri, Configuration config) {
    return getCacheManager(new ConfigSupplier(uri, config), new Properties());
  }

  /**
   * Enables to create a JSR-107 {@link CacheManager} based on the provided Ehcache {@link Configuration} with the
   * provided {@link Properties}.
   *
   * @param uri the URI identifying this cache manager
   * @param config the Ehcache configuration to use
   * @param properties extra properties
   *
   * @return a cache manager
   */
  public CacheManager getCacheManager(URI uri, Configuration config, Properties properties) {
    return getCacheManager(new ConfigSupplier(uri, config), properties);
  }

  Eh107CacheManager getCacheManager(ConfigSupplier configSupplier, Properties properties) {
    Eh107CacheManager cacheManager;
    ConcurrentMap<URI, Eh107CacheManager> byURI;
    final ClassLoader classLoader = configSupplier.getClassLoader();
    final URI uri = configSupplier.getUri();

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

        cacheManager = createCacheManager(uri, configSupplier.getConfiguration(), properties);
        byURI.put(uri, cacheManager);
      }
    }

    return cacheManager;
  }

  private Eh107CacheManager createCacheManager(URI uri, Configuration config, Properties properties) {
    Eh107CacheLoaderWriterProvider cacheLoaderWriterFactory = new Eh107CacheLoaderWriterProvider();

    Object[] serviceCreationConfigurations = config.getServiceCreationConfigurations().toArray();

    Jsr107Service jsr107Service = new DefaultJsr107Service(ServiceUtils.findSingletonAmongst(Jsr107Configuration.class, serviceCreationConfigurations));

    Collection<Service> services = new ArrayList<Service>(4);
    services.add(cacheLoaderWriterFactory);
    services.add(jsr107Service);

    if (ServiceUtils.findSingletonAmongst(DefaultSerializationProviderConfiguration.class, serviceCreationConfigurations) == null) {
      services.add(new DefaultJsr107SerializationProvider());
    }

    Eh107InternalCacheManager ehcacheManager = new Eh107InternalCacheManager(config, services, !jsr107Service.jsr107CompliantAtomics());
    ehcacheManager.init();

    return new Eh107CacheManager(this, ehcacheManager, properties, config.getClassLoader(), uri,
            new ConfigurationMerger(config, jsr107Service, cacheLoaderWriterFactory));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassLoader getDefaultClassLoader() {
    return ClassLoading.getDefaultClassLoader();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public URI getDefaultURI() {
    return URI_DEFAULT;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Properties getDefaultProperties() {
    return new Properties();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CacheManager getCacheManager(final URI uri, final ClassLoader classLoader) {
    return getCacheManager(uri, classLoader, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CacheManager getCacheManager() {
    return getCacheManager(getDefaultURI(), getDefaultClassLoader());
  }

  /**
   * {@inheritDoc}
   */
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

  /**
   * {@inheritDoc}
   */
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

  /**
   * {@inheritDoc}
   */
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

  /**
   * {@inheritDoc}
   */
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

  static class ConfigSupplier {
    private final URI uri;
    private final ClassLoader classLoader;
    private Configuration configuration;

    public ConfigSupplier(URI uri, ClassLoader classLoader) {
      this.uri = uri;
      this.classLoader = classLoader;
      this.configuration = null;
    }

    public ConfigSupplier(URI uri, Configuration configuration) {
      this.uri = uri;
      this.classLoader = configuration.getClassLoader();
      this.configuration = configuration;
    }

    public URI getUri() {
      return uri;
    }

    public ClassLoader getClassLoader() {
      return classLoader;
    }

    public Configuration getConfiguration() {
      if(configuration == null) {
        try {
          if (URI_DEFAULT.equals(uri)) {
            configuration = new DefaultConfiguration(classLoader);
          } else {
            configuration = new XmlConfiguration(uri.toURL(), classLoader);
          }
        } catch (Exception e) {
          throw new javax.cache.CacheException(e);
        }
      }
      return configuration;
    }
  }

}
