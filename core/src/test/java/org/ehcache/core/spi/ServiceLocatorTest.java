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
import java.util.Collection;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.ehcache.core.spi.services.TestMandatoryServiceFactory;
import org.ehcache.core.spi.services.ranking.RankServiceB;
import org.ehcache.core.spi.services.ranking.RankServiceA;
import org.ehcache.core.Ehcache;
import org.ehcache.core.spi.store.CacheProvider;
import org.ehcache.spi.service.OptionalServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.core.spi.services.DefaultTestProvidedService;
import org.ehcache.core.spi.services.DefaultTestService;
import org.ehcache.core.spi.services.FancyCacheProvider;
import org.ehcache.core.spi.services.TestProvidedService;
import org.ehcache.core.spi.services.TestService;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

/**
 * Tests for {@link ServiceLocator}.
 */
public class ServiceLocatorTest {

  @Test
  public void testClassHierarchies() {
    ServiceLocator.DependencySet dependencySet = dependencySet();
    final Service service = new ChildTestService();
    dependencySet.with(service);
    assertThat(dependencySet.providerOf(FooProvider.class), sameInstance(service));
    final Service fancyCacheProvider = new FancyCacheProvider();
    dependencySet.with(fancyCacheProvider);

    final Collection<CacheProvider> servicesOfType = dependencySet.providersOf(CacheProvider.class);
    assertThat(servicesOfType, is(not(empty())));
    assertThat(servicesOfType.iterator().next(), sameInstance(fancyCacheProvider));
  }

  @Test
  public void testDoesNotUseTCCL() {
    Thread.currentThread().setContextClassLoader(new ClassLoader() {
      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        throw new AssertionError();
      }
    });

    dependencySet().with(TestService.class).build().getService(TestService.class);
  }

  @Test
  public void testAttemptsToStopStartedServicesOnInitFailure() {
    Service s1 = new ParentTestService();
    FancyCacheProvider s2 = new FancyCacheProvider();

    ServiceLocator locator = dependencySet().with(s1).with(s2).build();
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

    ServiceLocator locator = dependencySet().with(s1).with(s2).with(s3).build();
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

    ServiceLocator locator = dependencySet().with(s1).build();
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
    ServiceLocator locator = dependencySet().with(new ExtendedTestService()).build();
    TestService testService = locator.getService(TestService.class);
    assertThat(testService, instanceOf(ExtendedTestService.class));
  }

  @Test
  public void testCanOverrideServiceDependencyWithoutOrderingProblem() throws Exception {
    final AtomicBoolean started = new AtomicBoolean(false);
    ServiceLocator serviceLocator = dependencySet().with(new TestServiceConsumerService())
      .with(new TestService() {
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
        started.set(true);
      }

      @Override
      public void stop() {
        // no-op
      }
    }).build();
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
    ServiceLocator.DependencySet dependencySet = dependencySet();

    // add some services
    dependencySet.with(consumer1);
    dependencySet.with(consumer2);
    dependencySet.with(new TestService() {
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
      }

      @Override
      public void stop() {
        // no-op
      }
    });

    // simulate what is done in ehcachemanager
    dependencySet.with(TestService.class);
    ServiceLocator serviceLocator = dependencySet.build();
    serviceLocator.startAllServices();

    serviceLocator.stopAllServices();

    verify(consumer1, times(1)).start(serviceLocator);
    verify(consumer1, times(1)).stop();

    assertThat(consumer2.testProvidedService.ctors(), greaterThanOrEqualTo(1));
    assertThat(consumer2.testProvidedService.stops(), equalTo(1));
    assertThat(consumer2.testProvidedService.starts(), equalTo(1));
  }

  @Test
  public void testRedefineDefaultServiceWhileDependingOnIt() throws Exception {
    ServiceLocator serviceLocator = dependencySet().with(new YetAnotherCacheProvider()).build();

    serviceLocator.startAllServices();
  }

  @Test(expected = IllegalStateException.class)
  public void testCircularDeps() throws Exception {

    final class StartStopCounter {
      final AtomicInteger startCounter = new AtomicInteger(0);
      final AtomicReference<ServiceProvider<Service>> startServiceProvider = new AtomicReference<>();
      final AtomicInteger stopCounter = new AtomicInteger(0);
      public void countStart(ServiceProvider<Service> serviceProvider) {
        startCounter.incrementAndGet();
        startServiceProvider.set(serviceProvider);
      }
      public void countStop() {
        stopCounter.incrementAndGet();
      }
    }

    @ServiceDependencies(TestProvidedService.class)
    class Consumer1 implements Service {
      final StartStopCounter startStopCounter = new StartStopCounter();
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
        assertThat(serviceProvider.getService(TestProvidedService.class), is(notNullValue()));
        startStopCounter.countStart(serviceProvider);
      }
      @Override
      public void stop() {
        startStopCounter.countStop();
      }
    }

    @ServiceDependencies(Consumer1.class)
    class Consumer2 implements Service {
      final StartStopCounter startStopCounter = new StartStopCounter();
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
        assertThat(serviceProvider.getService(Consumer1.class), is(notNullValue()));
        startStopCounter.countStart(serviceProvider);
      }
      @Override
      public void stop() {
        startStopCounter.countStop();
      }
    }

    @ServiceDependencies(Consumer2.class)
    class MyTestProvidedService extends DefaultTestProvidedService {
      final StartStopCounter startStopCounter = new StartStopCounter();
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
        assertThat(serviceProvider.getService(Consumer2.class), is(notNullValue()));
        startStopCounter.countStart(serviceProvider);
        super.start(serviceProvider);
      }
      @Override
      public void stop() {
        startStopCounter.countStop();
        super.stop();
      }
    }

    @ServiceDependencies(DependsOnMe.class)
    class DependsOnMe implements Service {
      final StartStopCounter startStopCounter = new StartStopCounter();
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
        assertThat(serviceProvider.getService(DependsOnMe.class), sameInstance(this));
        startStopCounter.countStart(serviceProvider);
      }
      @Override
      public void stop() {
        startStopCounter.countStop();
      }
    }

    ServiceLocator.DependencySet dependencySet = dependencySet();

    Consumer1 consumer1 = new Consumer1();
    Consumer2 consumer2 = new Consumer2();
    MyTestProvidedService myTestProvidedService = new MyTestProvidedService();
    DependsOnMe dependsOnMe = new DependsOnMe();

    // add some services
    dependencySet.with(consumer1);
    dependencySet.with(consumer2);
    dependencySet.with(myTestProvidedService);
    dependencySet.with(dependsOnMe);

    ServiceLocator serviceLocator = dependencySet.build();
    // simulate what is done in ehcachemanager
    serviceLocator.startAllServices();

    serviceLocator.stopAllServices();

    assertThat(consumer1.startStopCounter.startCounter.get(), is(1));
    assertThat(consumer1.startStopCounter.startServiceProvider.get(), CoreMatchers.<ServiceProvider<Service>>is(serviceLocator));
    assertThat(consumer2.startStopCounter.startCounter.get(), is(1));
    assertThat(consumer2.startStopCounter.startServiceProvider.get(), CoreMatchers.<ServiceProvider<Service>>is(serviceLocator));
    assertThat(myTestProvidedService.startStopCounter.startCounter.get(), is(1));
    assertThat(myTestProvidedService.startStopCounter.startServiceProvider.get(), CoreMatchers.<ServiceProvider<Service>>is(serviceLocator));
    assertThat(dependsOnMe.startStopCounter.startCounter.get(), is(1));
    assertThat(dependsOnMe.startStopCounter.startServiceProvider.get(), CoreMatchers.<ServiceProvider<Service>>is(serviceLocator));

    assertThat(consumer1.startStopCounter.stopCounter.get(), is(1));
    assertThat(consumer2.startStopCounter.stopCounter.get(), is(1));
    assertThat(myTestProvidedService.startStopCounter.stopCounter.get(), is(1));
    assertThat(dependsOnMe.startStopCounter.stopCounter.get(), is(1));
  }

  @Test
  public void testAbsentOptionalDepGetIgnored() {
    ServiceLocator serviceLocator = dependencySet().with(new ServiceWithOptionalDeps()).build();

    assertThat(serviceLocator.getService(ServiceWithOptionalDeps.class), is(notNullValue()));
    assertThat(serviceLocator.getService(TestService.class), is(notNullValue()));
    assertThat(serviceLocator.getService(OptService1.class), is(nullValue()));
    assertThat(serviceLocator.getService(OptService2.class), is(nullValue()));
  }

  @Test
  public void testPresentOptionalDepGetLoaded() {
    ServiceLocator serviceLocator = dependencySet().with(new ServiceWithOptionalDeps()).with(new OptService1()).with(new OptService2()).build();

    assertThat(serviceLocator.getService(ServiceWithOptionalDeps.class), is(notNullValue()));
    assertThat(serviceLocator.getService(TestService.class), is(notNullValue()));
    assertThat(serviceLocator.getService(OptService1.class), is(notNullValue()));
    assertThat(serviceLocator.getService(OptService2.class), is(notNullValue()));
  }

  @Test
  public void testMixedPresentAndAbsentOptionalDepGetLoadedAndIgnored() {
    ServiceLocator serviceLocator = dependencySet().with(new ServiceWithOptionalDeps()).with(new OptService2()).build();

    assertThat(serviceLocator.getService(ServiceWithOptionalDeps.class), is(notNullValue()));
    assertThat(serviceLocator.getService(TestService.class), is(notNullValue()));
    assertThat(serviceLocator.getService(OptService1.class), is(nullValue()));
    assertThat(serviceLocator.getService(OptService2.class), is(notNullValue()));
  }

  @Test
  public void testOptionalDepWithAbsentClass() {
    ServiceLocator serviceLocator = dependencySet().with(new ServiceWithOptionalNonExistentDeps()).with(new OptService2()).build();

    assertThat(serviceLocator.getService(ServiceWithOptionalNonExistentDeps.class), is(notNullValue()));
    assertThat(serviceLocator.getService(TestService.class), is(notNullValue()));
    assertThat(serviceLocator.getService(OptService2.class), is(notNullValue()));
  }

  @Test
  public void testManadatoryDependencyIsAddedToEmptySet() {
    ServiceLocator serviceLocator = dependencySet().build();

    TestMandatoryServiceFactory.TestMandatoryService service = serviceLocator.getService(TestMandatoryServiceFactory.TestMandatoryService.class);
    assertThat(service, notNullValue());
    assertThat(service.getConfig(), nullValue());
  }

  @Test
  public void testManadatoryDependenciesCanBeDisabled() {
    ServiceLocator serviceLocator = dependencySet().withoutMandatoryServices().build();

    assertThat(serviceLocator.getService(TestMandatoryServiceFactory.TestMandatoryService.class), nullValue());
  }

  @Test
  public void testMandatoryDependencyIsAddedToNonEmptySet() {
    ServiceLocator serviceLocator = dependencySet().with(new DefaultTestService()).build();

    TestMandatoryServiceFactory.TestMandatoryService service = serviceLocator.getService(TestMandatoryServiceFactory.TestMandatoryService.class);
    assertThat(service, notNullValue());
    assertThat(service.getConfig(), nullValue());
  }

  @Test
  public void testMandatoryDependencyCanStillBeRequested() {
    ServiceLocator serviceLocator = dependencySet().with(TestMandatoryServiceFactory.TestMandatoryService.class).build();

    TestMandatoryServiceFactory.TestMandatoryService service = serviceLocator.getService(TestMandatoryServiceFactory.TestMandatoryService.class);
    assertThat(service, notNullValue());
    assertThat(service.getConfig(), nullValue());
  }

  @Test
  public void testMandatoryDependencyWithProvidedConfigIsHonored() {
    ServiceLocator serviceLocator = dependencySet().with(new TestMandatoryServiceFactory.TestMandatoryServiceConfiguration("apple")).build();

    TestMandatoryServiceFactory.TestMandatoryService service = serviceLocator.getService(TestMandatoryServiceFactory.TestMandatoryService.class);
    assertThat(service, notNullValue());
    assertThat(service.getConfig(), is("apple"));
  }

  @Test
  public void testMandatoryDependencyCanBeDependedOn() {
    ServiceLocator serviceLocator = dependencySet().with(new NeedsMandatoryService()).build();

    TestMandatoryServiceFactory.TestMandatoryService service = serviceLocator.getService(TestMandatoryServiceFactory.TestMandatoryService.class);
    assertThat(service, notNullValue());
    assertThat(service.getConfig(), nullValue());
    assertThat(serviceLocator.getService(NeedsMandatoryService.class), notNullValue());
  }

  @Test
  public void testRankedServiceOverrides() {
    ServiceLocator serviceLocator = dependencySet().with(RankServiceA.class).build();
    assertThat(serviceLocator.getService(RankServiceA.class).getSource(), is("high-rank"));
  }

  @Test
  public void testRankedServiceOverridesMandatory() {
    ServiceLocator serviceLocator = dependencySet().build();
    assertThat(serviceLocator.getService(RankServiceA.class), nullValue());
  }

  @Test
  public void testRankedServiceBecomesMandatory() {
    ServiceLocator serviceLocator = dependencySet().build();
    assertThat(serviceLocator.getService(RankServiceB.class), notNullValue());
  }
}

@ServiceDependencies(TestService.class)
@OptionalServiceDependencies({
  "org.ehcache.core.spi.OptService1",
  "org.ehcache.core.spi.OptService2"})
class ServiceWithOptionalDeps implements Service {

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {

  }

  @Override
  public void stop() {

  }
}

@ServiceDependencies(TestService.class)
@OptionalServiceDependencies({
  "org.ehcache.core.internal.service.ServiceThatDoesNotExist",
  "org.ehcache.core.spi.OptService2"})
class ServiceWithOptionalNonExistentDeps implements Service {

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {

  }

  @Override
  public void stop() {

  }
}

class OptService1 implements Service {
  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
  }
  @Override
  public void stop() {
  }
}

class OptService2 implements Service {
  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
  }
  @Override
  public void stop() {
  }
}


@ServiceDependencies(FancyCacheProvider.class)
class YetAnotherCacheProvider implements CacheProvider {

  @Override
  public <K, V> Ehcache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceConfiguration<?, ?>... config) {
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

@ServiceDependencies(TestMandatoryServiceFactory.TestMandatoryService.class)
class NeedsMandatoryService implements TestService {

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {

  }

  @Override
  public void stop() {

  }
}
