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
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.SupplementaryService;
import org.ehcache.spi.services.DefaultTestService;
import org.ehcache.spi.services.TestService;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

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
  public void testAcceptsMultipleIdenticalServices() {
    ServiceLocator serviceLocator = new ServiceLocator();

    Service fancyCacheProvider = new FancyCacheProvider();
    DullCacheProvider dullCacheProvider = new DullCacheProvider();

    serviceLocator.addService(fancyCacheProvider);
    serviceLocator.addService(dullCacheProvider);

    assertThat(serviceLocator.findService(FooProvider.class), nullValue());
    assertThat(serviceLocator.findService(CacheProvider.class), sameInstance(fancyCacheProvider));
    assertThat(serviceLocator.findService(DullCacheProvider.class), sameInstance(dullCacheProvider));
  }
  
  @Test
  public void testDoesNotRegisterSupplementaryServiceUnderAbstractType() {
    ServiceLocator serviceLocator = new ServiceLocator();

    DullCacheProvider dullCacheProvider = new DullCacheProvider();

    serviceLocator.addService(dullCacheProvider);

    assertThat(serviceLocator.findService(FooProvider.class), nullValue());
    assertThat(serviceLocator.findService(CacheProvider.class), nullValue());
    assertThat(serviceLocator.findService(DullCacheProvider.class), sameInstance(dullCacheProvider));
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
    serviceLocator.findService(TestService.class);
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
    Service s3 = mock(CacheLoaderWriterProvider.class);

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

  @Test
  public void testStopAllServicesOnlyStopsEachServiceOnce() throws Exception {
    Service s1 = mock(CacheProvider.class, withSettings().extraInterfaces(CacheLoaderWriterProvider.class));

    ServiceLocator locator = new ServiceLocator(s1);
    try {
      locator.startAllServices();
    } catch (Exception e) {
      fail();
    }

    locator.stopAllServices();
    verify(s1, times(1)).stop();
  }

  @Test
  public void testCanOverrideDefaultServiceFromServiceLoader() {
    ServiceLocator locator = new ServiceLocator(new ExtendedTestService());
    TestService testService = locator.findService(TestService.class);
    assertThat(testService, instanceOf(ExtendedTestService.class));
  }
}

class ExtendedTestService extends DefaultTestService {

}

interface FooProvider extends Service {

}



class ParentTestService implements FooProvider {

  @Override
  public void start(final ServiceProvider serviceProvider) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void stop() { }

}

class ChildTestService extends ParentTestService {

  @Override
  public void start(final ServiceProvider serviceProvider) {
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
  public void start(final ServiceProvider serviceProvider) {
    ++startStopCounter;
  }

  @Override
  public void stop() {
    --startStopCounter;
  }
}

@SupplementaryService
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
  public void start(final ServiceProvider serviceProvider) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException();
  }
}
