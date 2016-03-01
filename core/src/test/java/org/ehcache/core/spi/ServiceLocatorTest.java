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

package org.ehcache.core.spi;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.ehcache.core.Ehcache;
import org.ehcache.core.spi.cache.CacheProvider;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.SupplementaryService;
import org.ehcache.core.spi.services.DefaultTestProvidedService;
import org.ehcache.core.spi.services.DefaultTestService;
import org.ehcache.core.spi.services.FancyCacheProvider;
import org.ehcache.core.spi.services.TestProvidedService;
import org.ehcache.core.spi.services.TestService;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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
    assertThat(provider.getService(FooProvider.class), sameInstance(service));
    final Service fancyCacheProvider = new FancyCacheProvider();
    provider.addService(fancyCacheProvider);
    assertThat(provider.getService(CacheProvider.class), sameInstance(fancyCacheProvider));
  }

  @Test
  public void testAcceptsMultipleIdenticalServices() {
    ServiceLocator serviceLocator = new ServiceLocator();

    Service fancyCacheProvider = new FancyCacheProvider();
    DullCacheProvider dullCacheProvider = new DullCacheProvider();

    serviceLocator.addService(fancyCacheProvider);
    serviceLocator.addService(dullCacheProvider);

    assertThat(serviceLocator.getService(FooProvider.class), nullValue());
    assertThat(serviceLocator.getService(CacheProvider.class), sameInstance(fancyCacheProvider));
    assertThat(serviceLocator.getService(DullCacheProvider.class), sameInstance(dullCacheProvider));
  }

  @Test
  public void testDoesNotRegisterSupplementaryServiceUnderAbstractType() {
    ServiceLocator serviceLocator = new ServiceLocator();

    DullCacheProvider dullCacheProvider = new DullCacheProvider();

    serviceLocator.addService(dullCacheProvider);

    assertThat(serviceLocator.getService(FooProvider.class), nullValue());
    assertThat(serviceLocator.getService(CacheProvider.class), nullValue());
    assertThat(serviceLocator.getService(DullCacheProvider.class), sameInstance(dullCacheProvider));
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
    serviceLocator.getService(TestService.class);
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
    TestService testService = locator.getService(TestService.class);
    assertThat(testService, instanceOf(ExtendedTestService.class));
  }

  @Test
  public void testCanOverrideServiceDependencyWithoutOrderingProblem() throws Exception {
    final AtomicBoolean started = new AtomicBoolean(false);
    ServiceLocator serviceLocator = new ServiceLocator(new TestServiceConsumerService());
    serviceLocator.addService(new TestService() {
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
        started.set(true);
      }

      @Override
      public void stop() {
        // no-op
      }
    });
    serviceLocator.startAllServices();
    assertThat(started.get(), is(true));
  }

  @Test
  public void testServicesInstanciatedOnceAndStartedOnce() throws Exception {

    @ServiceDependencies(TestProvidedService.class)
    class Consumer1 implements Service {
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
      }

      @Override
      public void stop() {

      }
    }

    @ServiceDependencies(TestProvidedService.class)
    class Consumer2 implements Service {
      TestProvidedService testProvidedService;
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
        testProvidedService = serviceProvider.getService(TestProvidedService.class);
      }

      @Override
      public void stop() {

      }
    }

    Consumer1 consumer1 = spy(new Consumer1());
    Consumer2 consumer2 = new Consumer2();
    ServiceLocator serviceLocator = new ServiceLocator();

    // add some services
    serviceLocator.addService(consumer1);
    serviceLocator.addService(consumer2);
    serviceLocator.addService(new TestService() {
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
      }

      @Override
      public void stop() {
        // no-op
      }
    });

    // simulate what is done in ehcachemanager
    serviceLocator.loadDependenciesOf(TestServiceConsumerService.class);
    serviceLocator.startAllServices();

    serviceLocator.stopAllServices();

    verify(consumer1, times(1)).start(serviceLocator);
    verify(consumer1, times(1)).stop();

    assertThat(consumer2.testProvidedService.ctors(), equalTo(1));
    assertThat(consumer2.testProvidedService.stops(), equalTo(1));
    assertThat(consumer2.testProvidedService.starts(), equalTo(1));
  }

  @Test
  public void testRedefineDefaultServiceWhileDependingOnIt() throws Exception {
    ServiceLocator serviceLocator = new ServiceLocator(new YetAnotherCacheProvider());

    serviceLocator.startAllServices();
  }

  @Test
  public void testCircularDeps() throws Exception {

    @ServiceDependencies(TestProvidedService.class)
    class Consumer1 implements Service {
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
        assertThat(serviceProvider.getService(TestProvidedService.class), is(notNull()));
      }
      @Override
      public void stop() {}
    }

    @ServiceDependencies(Consumer1.class)
    class Consumer2 implements Service {
      TestProvidedService testProvidedService;
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
        assertThat(serviceProvider.getService(Consumer1.class), is(notNull()));
      }
      @Override
      public void stop() {}
    }

    @ServiceDependencies(Consumer2.class)
    class MyTestProvidedService extends DefaultTestProvidedService {
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
        assertThat(serviceProvider.getService(Consumer2.class), is(notNull()));
        super.start(serviceProvider);
      }
    }

    @ServiceDependencies(DependsOnMe.class)
    class DependsOnMe implements Service {
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
        assertThat(serviceProvider.getService(DependsOnMe.class), sameInstance(this));;
      }
      @Override
      public void stop() {}
    }

    ServiceLocator serviceLocator = new ServiceLocator();

    Consumer1 consumer1 = mock(Consumer1.class);
    Consumer2 consumer2 = mock(Consumer2.class);
    MyTestProvidedService myTestProvidedService = mock(MyTestProvidedService.class);
    DependsOnMe dependsOnMe = mock(DependsOnMe.class);

    // add some services
    serviceLocator.addService(consumer1);
    serviceLocator.addService(consumer2);
    serviceLocator.addService(myTestProvidedService);
    serviceLocator.addService(dependsOnMe);

    // simulate what is done in ehcachemanager
    serviceLocator.startAllServices();

    serviceLocator.stopAllServices();

    verify(consumer1, times(1)).start(serviceLocator);
    verify(consumer2, times(1)).start(serviceLocator);
    verify(myTestProvidedService, times(1)).start(serviceLocator);
    verify(dependsOnMe, times(1)).start(serviceLocator);

    verify(consumer1, times(1)).stop();
    verify(consumer2, times(1)).stop();
    verify(myTestProvidedService, times(1)).stop();
    verify(dependsOnMe, times(1)).stop();
  }
}

@ServiceDependencies(FancyCacheProvider.class)
class YetAnotherCacheProvider implements CacheProvider {

  @Override
  public <K, V> Ehcache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceConfiguration<?>... config) {
    return null;
  }

  @Override
  public void releaseCache(Ehcache<?, ?> resource) {
    // no-op
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    // no-op
  }

  @Override
  public void stop() {
    // no-op
  }
}

class ExtendedTestService extends DefaultTestService {

}

interface FooProvider extends Service {

}

@ServiceDependencies(TestService.class)
class TestServiceConsumerService implements Service {

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    assertThat(serviceProvider.getService(TestService.class), notNullValue());
  }

  @Override
  public void stop() {
    // no-op
  }
}

class ParentTestService implements FooProvider {

  @Override
  public void start(final ServiceProvider<Service> serviceProvider) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void stop() { }

}

class ChildTestService extends ParentTestService {

  @Override
  public void start(final ServiceProvider<Service> serviceProvider) {
    throw new UnsupportedOperationException("Implement me!");
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
  public void start(final ServiceProvider<Service> serviceProvider) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException();
  }
}
