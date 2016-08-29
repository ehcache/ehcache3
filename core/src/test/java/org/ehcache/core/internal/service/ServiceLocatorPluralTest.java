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

package org.ehcache.core.internal.service;

import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests handling of {@link PluralService} by {@link ServiceLocator}.
 */
public class ServiceLocatorPluralTest {

  /**
   * Ensures that multiple instances of a single {@code Service} implementation <i>without</i>
   * the {@link PluralService} annotation can not have more than one instance registered.
   */
  @Test
  public void testMultipleInstanceRegistration() throws Exception {
    final ServiceLocator serviceLocator = new ServiceLocator();

    final ConcreteService firstSingleton = new ConcreteService();
    final ConcreteService secondSingleton = new ConcreteService();

    serviceLocator.addService(firstSingleton);

    assertThat(serviceLocator.getServicesOfType(ConcreteService.class), contains(firstSingleton));
    assertThat(serviceLocator.getServicesOfType(AdditionalService.class), Matchers.<AdditionalService>contains(firstSingleton));
    assertThat(serviceLocator.getServicesOfType(AggregateService.class), Matchers.<AggregateService>contains(firstSingleton));
    assertThat(serviceLocator.getServicesOfType(FooService.class), Matchers.<FooService>contains(firstSingleton));
    assertThat(serviceLocator.getServicesOfType(BarService.class), Matchers.<BarService>contains(firstSingleton));
    assertThat(serviceLocator.getServicesOfType(FoundationService.class), Matchers.<FoundationService>contains(firstSingleton));
    assertThat(serviceLocator.getServicesOfType(AugmentedService.class), Matchers.<AugmentedService>contains(firstSingleton));

    try {
      serviceLocator.addService(secondSingleton);
      fail();
    } catch (IllegalStateException e) {
      // expected
      assertThat(e.getMessage(), containsString("duplicate service class " + ConcreteService.class.getName()));
    }
  }

  /**
   * Ensures that multiple {@code Service} implementations of an interface <i>without</i> the
   * {@link PluralService} annotation can not both be registered.
   */
  @Test
  public void testMultipleImplementationRegistration() throws Exception {
    final ServiceLocator serviceLocator = new ServiceLocator();

    final ConcreteService firstSingleton = new ConcreteService();
    final ExtraConcreteService secondSingleton = new ExtraConcreteService();

    serviceLocator.addService(firstSingleton);

    assertThat(serviceLocator.getServicesOfType(ConcreteService.class), contains(firstSingleton));
    assertThat(serviceLocator.getServicesOfType(AdditionalService.class), Matchers.<AdditionalService>contains(firstSingleton));
    assertThat(serviceLocator.getServicesOfType(AggregateService.class), Matchers.<AggregateService>contains(firstSingleton));
    assertThat(serviceLocator.getServicesOfType(FooService.class), Matchers.<FooService>contains(firstSingleton));
    assertThat(serviceLocator.getServicesOfType(BarService.class), Matchers.<BarService>contains(firstSingleton));
    assertThat(serviceLocator.getServicesOfType(FoundationService.class), Matchers.<FoundationService>contains(firstSingleton));
    assertThat(serviceLocator.getServicesOfType(AugmentedService.class), Matchers.<AugmentedService>contains(firstSingleton));

    try {
      serviceLocator.addService(secondSingleton);
      fail();
    } catch (IllegalStateException e) {
      // expected

      // This assertion is here to point out a potentially unwanted side-effect -- a partial registration
      assertThat(serviceLocator.getServicesOfType(ExtraConcreteService.class), contains(secondSingleton));

      final String message = e.getMessage();
      assertThat(message, containsString(AdditionalService.class.getName()));
      assertThat(message, containsString(AggregateService.class.getName()));
      assertThat(message, containsString(FooService.class.getName()));
      assertThat(message, containsString(BarService.class.getName()));
      assertThat(message, containsString(FoundationService.class.getName()));
      assertThat(message, containsString(AugmentedService.class.getName()));
    }
  }

  /**
   * Ensures that multiple {@code Service} implementations of an interface <i>with</i> the
   * {@link PluralService} annotation can both be registered.
   */
  @Test
  public void testPluralRegistration() throws Exception {
    final ServiceLocator serviceLocator = new ServiceLocator();

    final AlphaServiceProviderImpl alphaServiceProvider = new AlphaServiceProviderImpl();
    final BetaServiceProviderImpl betaServiceProvider = new BetaServiceProviderImpl();

    serviceLocator.addService(alphaServiceProvider);

    assertThat(serviceLocator.getServicesOfType(AlphaServiceProviderImpl.class),
        everyItem(isOneOf(alphaServiceProvider)));
    assertThat(serviceLocator.getServicesOfType(AlphaServiceProvider.class),
        everyItem(Matchers.<AlphaServiceProvider>isOneOf(alphaServiceProvider)));
    assertThat(serviceLocator.getServicesOfType(PluralServiceProvider.class),
        everyItem(Matchers.<PluralServiceProvider>isOneOf(alphaServiceProvider)));

    serviceLocator.addService(betaServiceProvider);

    assertThat(serviceLocator.getServicesOfType(BetaServiceProviderImpl.class),
        everyItem(isOneOf(betaServiceProvider)));
    assertThat(serviceLocator.getServicesOfType(BetaServiceProvider.class),
        everyItem(Matchers.<BetaServiceProvider>isOneOf(betaServiceProvider)));
    assertThat(serviceLocator.getServicesOfType(PluralServiceProvider.class),
        everyItem(Matchers.<PluralServiceProvider>isOneOf(alphaServiceProvider, betaServiceProvider)));
  }

  /**
   * Ensures dependencies declared in {@link ServiceDependencies} on a {@code Service} subtype
   * can be discovered.
   */
  @Test
  public void testDependencyDiscoveryOverService() throws Exception {
    final ServiceLocator serviceLocator = new ServiceLocator();

    final Collection<Class<? extends Service>> concreteServiceDependencies =
        serviceLocator.identifyTransitiveDependenciesOf(ConcreteService.class);
    assertThat(concreteServiceDependencies,
        everyItem(Matchers.<Class<? extends Service>>isOneOf(
            BetaService.class,
            BetaServiceProvider.class,
            InitialService.class,
            FooService.Provider.class,
            BarService.Provider.class,
            AlphaService.class,
            AlphaServiceProvider.class,
            FoundationService.Provider.class
        )));
  }

  /**
   * Ensures dependencies declared in {@link ServiceDependencies} on a non-{@code Service} type
   * can be discovered.
   */
  @Test
  public void testDependencyDiscoveryOverNonService() throws Exception {
    final ServiceLocator serviceLocator = new ServiceLocator();

    final Collection<Class<? extends Service>> nonServiceDependencies =
        serviceLocator.identifyTransitiveDependenciesOf(NotAService.class);
    System.out.printf("NotAService dependencies : %s%n", nonServiceDependencies);
    assertThat(nonServiceDependencies,
        everyItem(Matchers.<Class<? extends Service>>isOneOf(
            BetaService.class,
            BetaServiceProvider.class,
            AlphaService.class,
            AlphaServiceProvider.class
        )));
  }

}

class StartStopCounter {
  private AtomicInteger startCounter = new AtomicInteger(0);
  private AtomicInteger stopCounter = new AtomicInteger(0);
  private AtomicReference<ServiceProvider<Service>> serviceProvider = new AtomicReference<ServiceProvider<Service>>();

  public void countStart(final ServiceProvider<Service> serviceProvider) {
    this.startCounter.incrementAndGet();
    this.serviceProvider.set(serviceProvider);
  }

  public void countStop() {
    this.stopCounter.incrementAndGet();
  }

  public int getStartCounter() {
    return startCounter.get();
  }

  public int getStopCounter() {
    return stopCounter.get();
  }

  public ServiceProvider<Service> getServiceProvider() {
    return serviceProvider.get();
  }
}

@PluralService
interface PluralServiceProvider extends Service {
}

/**
 * Top-level, concrete test class; multiples <b>not</b> permitted.
 * <p>
 * Inherits from
 * <pre>{@code
 *     <- AbstractService                   @ServiceDependencies({ BetaService })
 *                                              => @ServiceDependencies({ BetaServiceProvider })
 *         <- CoreService                   @ServiceDependencies({ AlphaService })
 *                                              => @ServiceDependencies({ AlphaServiceProvider })
 *             <- BaseService
 *             <- FoundationService         @ServiceDependencies({ InitialService, FoundationService.Provider })
 *                 <- Service
 *         <- AugmentedService
 *             <- AggregateService
 *                 <- FooService        @ServiceDependencies({ FooService.Provider })
 *                     <- Service
 *                 <- BarService        @ServiceDependencies({ BarService.Provider })
 *                     <- Service
 *             <- AdditionalService     @ServiceDependencies({ InitialService })
 *                 <- Service
 * }</pre>
 */
class ConcreteService extends AbstractService {
}

class ExtraConcreteService extends AbstractService {
}

@ServiceDependencies({ BetaService.class })
class AbstractService extends CoreService
    implements AugmentedService {
}

@ServiceDependencies({ AlphaService.class })
class CoreService extends BaseService implements FoundationService {
}

// This class must *not* be annotated with @PluralService!
class BaseService implements Service {
  final StartStopCounter counter = new StartStopCounter();

  @Override
  public void start(final ServiceProvider<Service> serviceProvider) {
    this.counter.countStart(serviceProvider);
  }

  @Override
  public void stop() {
    this.counter.countStop();
  }
}

interface AugmentedService extends AdditionalService, AggregateService {
}

@ServiceDependencies({ InitialService.class })
interface AdditionalService extends Service {
}

@ServiceDependencies({ InitialService.class, FoundationService.Provider.class })
interface FoundationService extends Service {
  interface Provider extends Service {
  }
}

interface AggregateService extends FooService, BarService {
}

@ServiceDependencies({ FooService.Provider.class })
interface FooService extends Service {
  interface Provider extends Service {
  }
}

@ServiceDependencies({ BarService.Provider.class })
interface BarService extends Service {
  interface Provider extends Service {
  }
}

@ServiceDependencies({ AlphaServiceProvider.class })
interface AlphaService extends Service, NonServiceAlpha {
}

@ServiceDependencies({ BetaServiceProvider.class })
interface BetaService extends Service, NonServiceBeta {
}

interface InitialService extends Service {
}

interface AlphaServiceProvider extends PluralServiceProvider {
}

class AlphaServiceProviderImpl extends BaseService implements AlphaServiceProvider {
}

interface BetaServiceProvider extends PluralServiceProvider {
}

class BetaServiceProviderImpl extends BaseService implements BetaServiceProvider {
}

interface NonServiceAlpha {
}

interface NonServiceBeta {
}

@ServiceDependencies({ AlphaService.class })
class NotAService extends AlsoNotAService {
}

@ServiceDependencies({ BetaService.class })
class AlsoNotAService  implements NotAServiceInterface {
}

interface NotAServiceInterface {
}

interface NonService {
}
