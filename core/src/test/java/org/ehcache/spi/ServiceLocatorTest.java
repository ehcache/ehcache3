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

package org.ehcache.spi;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

import org.ehcache.Ehcache;
import org.ehcache.spi.cache.CacheProvider;
import org.ehcache.spi.loader.CacheLoaderFactory;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Alex Snaps
 */
public class ServiceLocatorTest {

  @Test
  public void testClassHierarchies() {
    ServiceLocator provider = new ServiceLocator();
    final Service service = new ChildTestService();
    provider.addService(service);
    assertThat(provider.findService(FooProvider.class), sameInstance(service));
    final Service fancyCacheProvider = new FancyCacheProvider();
    provider.addService(fancyCacheProvider);
    assertThat(provider.findService(CacheProvider.class), sameInstance(fancyCacheProvider));
  }

  @Test
  public void testThrowsWhenMultipleIdenticalServicesAdded() {
    ServiceLocator provider = new ServiceLocator();
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
  
  
  @Test
  public void testDoesNotUseTCCL() {
    Thread.currentThread().setContextClassLoader(new ClassLoader() {
      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        throw new AssertionError();
      }
    });
    
    ServiceLocator serviceLocator = new ServiceLocator();
    serviceLocator.discoverService(Service.class);
  }

  @Test
  public void testAttemptsToStopStartedServicesOnInitFailure() {
    Service s1 = new ParentTestService();
    FancyCacheProvider s2 = new FancyCacheProvider();

    ServiceLocator locator = new ServiceLocator(s1, s2);
    try {
      locator.startAllServices();
      fail();
    } catch (Exception e) {
      // see org.ehcache.spi.ParentTestService.start()
      assertThat(e, instanceOf(RuntimeException.class));
      assertThat(e.getMessage(), is("Implement me!"));
    }
    assertThat(s2.startStopCounter, is(0));
  }

  @Test
  public void testAttemptsToStopAllServicesOnCloseFailure() {
    Service s1 = mock(CacheProvider.class);
    Service s2 = mock(FooProvider.class);
    Service s3 = mock(CacheLoaderFactory.class);

    ServiceLocator locator = new ServiceLocator(s1, s2, s3);
    try {
      locator.startAllServices();
    } catch (Exception e) {
      fail();
    }
    final RuntimeException thrown = new RuntimeException();
    doThrow(thrown).when(s1).stop();

    try {
      locator.stopAllServices();
      fail();
    } catch (Exception e) {
      assertThat(e, CoreMatchers.<Exception>sameInstance(thrown));
    }
    verify(s1).stop();
    verify(s2).stop();
    verify(s3).stop();
  }
}

interface FooProvider extends Service {

}



class ParentTestService implements FooProvider {

  @Override
  public void start() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void stop() { }

}

class ChildTestService extends ParentTestService {

  @Override
  public void start() {
    throw new UnsupportedOperationException("Implement me!");
  }
}

class FancyCacheProvider implements CacheProvider {

  int startStopCounter = 0;

  @Override
  public <K, V> Ehcache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceConfiguration<?>... config) {
    return null;
  }

  @Override
  public void releaseCache(final Ehcache<?, ?> resource) {
    //
  }

  @Override
  public void start() {
    ++startStopCounter;
  }

  @Override
  public void stop() {
    --startStopCounter;
  }
}

class DullCacheProvider implements CacheProvider {
  @Override
  public <K, V> Ehcache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceConfiguration<?>... config) {
    return null;
  }

  @Override
  public void releaseCache(final Ehcache<?, ?> resource) {
    //
  }

  @Override
  public void start() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException();
  }
}
