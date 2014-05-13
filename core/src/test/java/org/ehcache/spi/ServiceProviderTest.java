/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.spi;

import java.util.concurrent.Future;

import org.ehcache.internal.ServiceLocator;
import org.ehcache.spi.cache.CacheProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Test;
import org.ehcache.Cache;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Alex Snaps
 */
public class ServiceProviderTest {

  @Test
  public void testClassHierarchies() {
    ServiceProvider provider = new ServiceProvider();
    final Service service = new ChildTestService();
    provider.addService(service);
    assertThat(provider.findService(FooProvider.class), sameInstance(service));
    final Service fancyCacheProvider = new FancyCacheProvider();
    provider.addService(fancyCacheProvider);
    assertThat(provider.findService(CacheProvider.class), sameInstance(fancyCacheProvider));
  }

  @Test
  public void testThrowsWhenMultipleIdenticalServicesAdded() {
    ServiceProvider provider = new ServiceProvider();
    final Service service = new FancyCacheProvider();
    provider.addService(service);

    try {
      provider.addService(new FancyCacheProvider());
      fail();
    } catch (IllegalStateException e) {
      // expected
    }

    try {
      provider.addService(new DullCacheProvider());
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
    assertThat(provider.findService(FooProvider.class), nullValue());
    assertThat(provider.findService(CacheProvider.class), sameInstance(service));
  }
}

interface FooProvider extends Service {

}



class ParentTestService implements FooProvider {

  @Override
  public Future<?> start() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<?> stop() {
    throw new UnsupportedOperationException();
  }
}

class ChildTestService extends ParentTestService {

}

class FancyCacheProvider implements CacheProvider {

  @Override
  public <K, V> Cache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceLocator serviceProvider, ServiceConfiguration<?>... config) {
    return null;
  }

  @Override
  public void releaseCache(final Cache<?, ?> resource) {
    //
  }

  @Override
  public Future<?> start() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<?> stop() {
    throw new UnsupportedOperationException();
  }
}

class DullCacheProvider implements CacheProvider {
  @Override
  public <K, V> Cache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceLocator serviceProvider, ServiceConfiguration<?>... config) {
    return null;
  }

  @Override
  public void releaseCache(final Cache<?, ?> resource) {
    //
  }

  @Override
  public Future<?> start() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<?> stop() {
    throw new UnsupportedOperationException();
  }
}
