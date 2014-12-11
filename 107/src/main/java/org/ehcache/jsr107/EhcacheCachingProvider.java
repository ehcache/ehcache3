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
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;

import org.ehcache.EhcacheManager;
import org.ehcache.config.xml.XmlConfiguration;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.util.ClassLoading;

/**
 * @author teck
 */
public class EhcacheCachingProvider implements CachingProvider {

  private static final URI URI_DEFAULT;

  private final Map<ClassLoader, ConcurrentMap<URI, Eh107CacheManager>> cacheManagers = new WeakHashMap<ClassLoader, ConcurrentMap<URI, Eh107CacheManager>>();

  static {
    URI uri;
    try {
      URL resource = EhcacheCachingProvider.class.getResource("/ehcache.xml");
      if (resource == null) {
        resource = EhcacheCachingProvider.class.getResource("/ehcache-failsafe.xml");
      }

      if (resource == null) {
        throw new javax.cache.CacheException("No default URI could be found for ehcache");
      }

      uri = new URI(resource.toString());
    } catch (URISyntaxException e) {
      throw new javax.cache.CacheException(e);
    }

    URI_DEFAULT = uri;
  }

  @Override
  public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
    uri = uri == null ? getDefaultURI() : uri;
    classLoader = classLoader == null ? getDefaultClassLoader() : classLoader;
    properties = properties == null ? new Properties() : new Properties(properties);

    Eh107CacheManager cacheManager;
    ConcurrentMap<URI, Eh107CacheManager> byURI;

    synchronized (cacheManagers) {
      byURI = cacheManagers.get(classLoader);
      if (byURI == null) {
        byURI = new ConcurrentHashMap<URI, Eh107CacheManager>();
        cacheManagers.put(classLoader, byURI);
      }

      cacheManager = byURI.get(uri);
      if (cacheManager == null) {
        XmlConfiguration config;
        try {
          config = new XmlConfiguration(uri.toURL(), classLoader);
        } catch (Exception e) {
          throw new javax.cache.CacheException(e);
        }

        Eh107CacheLoaderFactory cacheLoaderFactory = new Eh107CacheLoaderFactory();
        Eh107CacheWriterFactory cacheWriterFactory = new Eh107CacheWriterFactory();
        Jsr107Service jsr107Service = new DefaultJsr107Service();

        ServiceLocator serviceLocator = new ServiceLocator();
        serviceLocator.addService(cacheLoaderFactory);
        serviceLocator.addService(cacheWriterFactory);
        serviceLocator.addService(jsr107Service);
       
        org.ehcache.CacheManager ehcacheManager = new EhcacheManager(config, serviceLocator);
        ehcacheManager.init();
        cacheManager = new Eh107CacheManager(this, ehcacheManager, properties, classLoader, uri, cacheLoaderFactory,
            cacheWriterFactory, config, jsr107Service);
        byURI.put(uri, cacheManager);
      }
    }

    return cacheManager;
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
}
