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
