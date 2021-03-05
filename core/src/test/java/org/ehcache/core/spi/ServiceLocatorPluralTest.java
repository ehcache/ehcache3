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

import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
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
    final ServiceLocator.DependencySet serviceLocator = dependencySet();

    final ConcreteService firstSingleton = new ConcreteService();
    final ConcreteService secondSingleton = new ConcreteService();

    serviceLocator.with(firstSingleton);

    assertThat(serviceLocator.providersOf(ConcreteService.class), contains(firstSingleton));
    assertThat(serviceLocator.providersOf(AdditionalService.class), Matchers.<AdditionalService>contains(firstSingleton));
    assertThat(serviceLocator.providersOf(AggregateService.class), Matchers.<AggregateService>contains(firstSingleton));
    assertThat(serviceLocator.providersOf(FooService.class), Matchers.<FooService>contains(firstSingleton));
    assertThat(serviceLocator.providersOf(BarService.class), Matchers.<BarService>contains(firstSingleton));
    assertThat(serviceLocator.providersOf(FoundationService.class), Matchers.<FoundationService>contains(firstSingleton));
    assertThat(serviceLocator.providersOf(AugmentedService.class), Matchers.<AugmentedService>contains(firstSingleton));

    try {
      serviceLocator.with(secondSingleton);
      fail();
    } catch (IllegalStateException e) {
      // expected
      assertThat(e.getMessage(), containsString(ConcreteService.class.getName()));
    }
  }

  /**
   * Ensures that multiple {@code Service} implementations of an interface <i>without</i> the
   * {@link PluralService} annotation can not both be registered.
   */
  @Test
  public void testMultipleImplementationRegistration() throws Exception {
    final ServiceLocator.DependencySet serviceLocator = dependencySet();

    final ConcreteService firstSingleton = new ConcreteService();
    final ExtraConcreteService secondSingleton = new ExtraConcreteService();

    serviceLocator.with(firstSingleton);

    assertThat(serviceLocator.providersOf(ConcreteService.class), contains(firstSingleton));
    assertThat(serviceLocator.providersOf(AdditionalService.class), Matchers.<AdditionalService>contains(firstSingleton));
    assertThat(serviceLocator.providersOf(AggregateService.class), Matchers.<AggregateService>contains(firstSingleton));
    assertThat(serviceLocator.providersOf(FooService.class), Matchers.<FooService>contains(firstSingleton));
    assertThat(serviceLocator.providersOf(BarService.class), Matchers.<BarService>contains(firstSingleton));
    assertThat(serviceLocator.providersOf(FoundationService.class), Matchers.<FoundationService>contains(firstSingleton));
    assertThat(serviceLocator.providersOf(AugmentedService.class), Matchers.<AugmentedService>contains(firstSingleton));

    try {
      serviceLocator.with(secondSingleton);
      fail();
    } catch (IllegalStateException e) {
      // expected
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
    final ServiceLocator.DependencySet dependencySet = dependencySet();

    final AlphaServiceProviderImpl alphaServiceProvider = new AlphaServiceProviderImpl();
    final BetaServiceProviderImpl betaServiceProvider = new BetaServiceProviderImpl();

    dependencySet.with(alphaServiceProvider);

    assertThat(dependencySet.providersOf(AlphaServiceProviderImpl.class),
        everyItem(isOneOf(alphaServiceProvider)));
    assertThat(dependencySet.providersOf(AlphaServiceProvider.class),
        everyItem(Matchers.<AlphaServiceProvider>isOneOf(alphaServiceProvider)));
    assertThat(dependencySet.providersOf(PluralServiceProvider.class),
        everyItem(Matchers.<PluralServiceProvider>isOneOf(alphaServiceProvider)));

    dependencySet.with(betaServiceProvider);

    assertThat(dependencySet.providersOf(BetaServiceProviderImpl.class),
        everyItem(isOneOf(betaServiceProvider)));
    assertThat(dependencySet.providersOf(BetaServiceProvider.class),
        everyItem(Matchers.<BetaServiceProvider>isOneOf(betaServiceProvider)));
    assertThat(dependencySet.providersOf(PluralServiceProvider.class),
        everyItem(Matchers.<PluralServiceProvider>isOneOf(alphaServiceProvider, betaServiceProvider)));
  }
}

class StartStopCounter {
  private AtomicInteger startCounter = new AtomicInteger(0);
  private AtomicInteger stopCounter = new AtomicInteger(0);
  private AtomicReference<ServiceProvider<Service>> serviceProvider = new AtomicReference<>();

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
