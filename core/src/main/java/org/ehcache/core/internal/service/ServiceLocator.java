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
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.core.internal.util.ClassLoading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Provides discovery and tracking services for {@link Service} implementations.
 */
public final class ServiceLocator implements ServiceProvider<Service> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceLocator.class);
  private final ConcurrentMap<Class<? extends Service>, Set<Service>> services =
      new ConcurrentHashMap<Class<? extends Service>, Set<Service>>();

  @SuppressWarnings("rawtypes")
  private final ServiceLoader<ServiceFactory> serviceFactory = ClassLoading.libraryServiceLoaderFor(ServiceFactory.class);

  private final ReadWriteLock runningLock = new ReentrantReadWriteLock();

  private final AtomicBoolean running = new AtomicBoolean(false);

  public ServiceLocator(Service... services) {
    for (Service service : services) {
      addService(service);
    }
  }

  /**
   * For the {@link Service} class specified, attempt to instantiate the service using the
   * {@link ServiceFactory} infrastructure.  If a configuration is provided, only the first
   * discovered factory is used to instantiate one copy of the service; if no configuration
   * is provided, use each discovered factory for the service type to attempt to create a
   * service from that factory.
   *
   * @param serviceClass the {@code Service} type to create
   * @param config the service configuration to use; may be null
   * @param <T> the type of the {@code Service}
   *
   * @return the collection of created services; may be empty
   *
   * @throws IllegalStateException if the configured service is already registered or the configured service
   *        implements a {@code Service} subtype that is not marked with the {@link PluralService} annotation
   *        but is already registered
   */
  private <T extends Service> Collection<T> discoverServices(Class<T> serviceClass, ServiceCreationConfiguration<T> config) {
    final List<T> addedServices = new ArrayList<T>();
    for (ServiceFactory<T> factory : ServiceLocator.<T> getServiceFactories(serviceFactory)) {
      final Class<T> factoryServiceType = factory.getServiceType();
      if (serviceClass.isAssignableFrom(factoryServiceType)) {
        if (services.containsKey(factoryServiceType)) {
          // Can have only one service registered under a concrete type
          continue;
        }
        T service = factory.create(config);
        addService(service);
        addedServices.add(service);
        if (config != null) {
          // Each configuration should be manifested in exactly one service; look no further
          return addedServices;
        }
      }
    }
    return addedServices;
  }

  @SuppressWarnings("unchecked")
  private static <T extends Service> Iterable<ServiceFactory<T>> getServiceFactories(@SuppressWarnings("rawtypes") ServiceLoader<ServiceFactory> serviceFactory) {
    List<ServiceFactory<T>> list = new ArrayList<ServiceFactory<T>>();
    for (ServiceFactory<?> factory : serviceFactory) {
      list.add((ServiceFactory<T>)factory);
    }
    return list;
  }

  /**
   * Registers the {@code Service} provided with this {@code ServiceLocator}.  If the service is
   * already registered, the registration fails.  The service specified is also registered under
   * each {@code Service} subtype it implements.  Duplicate registration of implemented {@code Service}
   * subtypes causes registration failure unless that subtype is marked with the {@link PluralService}
   * annotation.
   *
   * @param service the concrete {@code Service} to register
   *
   * @throws IllegalStateException if the configured service is already registered or {@code service}
   *        implements a {@code Service} subtype that is not marked with the {@link PluralService} annotation
   *        but is already registered
   */
  public void addService(final Service service) {
    final Lock lock = runningLock.readLock();
    lock.lock();
    try {
      Set<Class<? extends Service>> serviceClazzes = new HashSet<Class<? extends Service>>();

      for (Class<?> i : getAllInterfaces(service.getClass())) {
        if (Service.class != i && Service.class.isAssignableFrom(i)) {

          @SuppressWarnings("unchecked")
          Class<? extends Service> serviceClass = (Class<? extends Service>) i;

          serviceClazzes.add(serviceClass);
        }
      }

      if (services.putIfAbsent(service.getClass(), Collections.singleton(service)) != null) {
        throw new IllegalStateException("Registration of duplicate service " + service.getClass());
      }

      /*
       * Register the concrete service under all Service subtypes it implements.  If
       * the Service subtype is annotated with @PluralService, permit multiple registrations;
       * otherwise, fail the registration,
       */
      for (Class<? extends Service> serviceClazz : serviceClazzes) {
        if (serviceClazz.isAnnotationPresent(PluralService.class)) {
          // Permit multiple registrations
          Set<Service> registeredServices = services.get(serviceClazz);
          if (registeredServices == null) {
            registeredServices = new LinkedHashSet<Service>();
            services.put(serviceClazz, registeredServices);
          }
          registeredServices.add(service);

        } else {
          // Only a single registration permitted
          if (services.putIfAbsent(serviceClazz, Collections.singleton(service)) != null) {
            final StringBuilder message = new StringBuilder("Duplicate service implementation(s) found for ")
                .append(service.getClass());
            for (Class<? extends Service> serviceClass : serviceClazzes) {
              if (!serviceClass.isAnnotationPresent(PluralService.class)) {
                final Service declaredService = services.get(serviceClass).iterator().next();
                if (declaredService != null) {
                  message
                      .append("\n\t\t- ")
                      .append(serviceClass)
                      .append(" already has ")
                      .append(declaredService.getClass());
                }
              }
            }
            throw new IllegalStateException(message.toString());
          }
        }
      }

      if (running.get()) {
        loadDependenciesOf(service.getClass());
        service.start(this);
      }
    } finally {
      lock.unlock();
    }
  }

  private Collection<Class<?>> getAllInterfaces(final Class<?> clazz) {
    ArrayList<Class<?>> interfaces = new ArrayList<Class<?>>();
    for(Class<?> c = clazz; c != null; c = c.getSuperclass()) {
      for (Class<?> i : c.getInterfaces()) {
        interfaces.add(i);
        interfaces.addAll(getAllInterfaces(i));
      }
    }
    return interfaces;
  }

  /**
   * Obtains the service supporting the configuration provided.  If a registered service
   * is not available, an attempt to create the service using the {@link ServiceFactory}
   * discovery process will be made.
   *
   * @param config the configuration for the service
   * @param <T> the expected service type
   * @return the service instance for {@code T} type, or {@code null} if it couldn't be located or instantiated
   *
   * @throws IllegalArgumentException if {@link ServiceCreationConfiguration#getServiceType() config.getServiceType()}
   *        is marked with the {@link org.ehcache.spi.service.PluralService PluralService} annotation
   */
  public <T extends Service> T getOrCreateServiceFor(ServiceCreationConfiguration<T> config) {
    return getServiceInternal(config.getServiceType(), config, true);
  }

  /**
   * Obtains the identified service.  If a registered service is not available, an attempt
   * to create the service using the {@link ServiceFactory} discovery process will be made.
   *
   * @param serviceType the {@code class} of the service being looked up
   * @param <T> the expected service type
   * @return the service instance for {@code T} type, or {@code null} if a {@code Service} of type
   *        {@code serviceType} is not available
   *
   * @throws IllegalArgumentException if {@code serviceType} is marked with the
   *        {@link org.ehcache.spi.service.PluralService PluralService} annotation;
   *        use {@link #getServicesOfType(Class)} for plural services
   */
  @Override
  public <T extends Service> T getService(Class<T> serviceType) {
    return getServiceInternal(serviceType, null, false);
  }

  private <T extends Service> T getServiceInternal(
      final Class<T> serviceType, final ServiceCreationConfiguration<T> config, final boolean shouldCreate) {
    if (serviceType.isAnnotationPresent(PluralService.class)) {
      throw new IllegalArgumentException(serviceType.getName() + " is marked as a PluralService");
    }
    final Collection<T> registeredServices = findServices(serviceType, config, shouldCreate);
    if (registeredServices.size() > 1) {
      throw new AssertionError("The non-PluralService type" + serviceType.getName()
          + " has more than one service registered");
    }
    return (registeredServices.isEmpty() ? null : registeredServices.iterator().next());
  }

  private <T extends Service> Collection<T> findServices(
      Class<T> serviceType, ServiceCreationConfiguration<T> config, boolean shouldCreate) {
    final Collection<T> registeredServices = getServicesOfTypeInternal(serviceType);
    if (shouldCreate && (registeredServices.isEmpty() || serviceType.isAnnotationPresent(PluralService.class))) {
      registeredServices.addAll(discoverServices(serviceType, config));
    }
    return registeredServices;
  }

  public static <T> Collection<T> findAmongst(Class<T> clazz, Collection<?> instances) {
    return findAmongst(clazz, instances.toArray());
  }

  public static <T> Collection<T> findAmongst(Class<T> clazz, Object ... instances) {
    Collection<T> matches = new ArrayList<T>();
    for (Object instance : instances) {
      if (instance != null && clazz.isAssignableFrom(instance.getClass())) {
        matches.add(clazz.cast(instance));
      }
    }
    return Collections.unmodifiableCollection(matches);
  }

  public static <T> T findSingletonAmongst(Class<T> clazz, Collection<?> instances) {
    return findSingletonAmongst(clazz, instances.toArray());
  }

  public static <T> T findSingletonAmongst(Class<T> clazz, Object ... instances) {
    final Collection<T> matches = findAmongst(clazz, instances);
    if (matches.isEmpty()) {
      return null;
    } else if (matches.size() == 1) {
      return matches.iterator().next();
    } else {
      throw new IllegalArgumentException("More than one " + clazz.getName() + " found");
    }
  }

  public void startAllServices() throws Exception {
    Deque<Service> started = new ArrayDeque<Service>();
    final Lock lock = runningLock.writeLock();
    lock.lock();
    try {
      resolveMissingDependencies();

      if (!running.compareAndSet(false, true)) {
        throw new IllegalStateException("Already started!");
      }

      for (Set<Service> registeredServices : services.values()) {
        for (Service service : registeredServices) {
          if (!started.contains(service)) {
            service.start(this);
            started.push(service);
          }
        }
      }
      LOGGER.debug("All Services successfully started.");
    } catch (Exception e) {
      while(!started.isEmpty()) {
        Service toBeStopped = started.pop();
        try {
          toBeStopped.stop();
        } catch (Exception e1) {
          LOGGER.error("Stopping Service failed due to ", e1);
        }
      }
      throw e;
    } finally {
      lock.unlock();
    }
  }

  private void resolveMissingDependencies() {
    for (Set<Service> registeredServices : services.values()) {
      for (Service service : registeredServices) {
        loadDependenciesOf(service.getClass());
      }
    }
  }

  public void stopAllServices() throws Exception {
    Exception firstException = null;
    Lock lock = runningLock.writeLock();
    lock.lock();
    try {
      if(!running.compareAndSet(true, false)) {
        throw new IllegalStateException("Already stopped!");
      }
      Set<Service> stoppedServices = Collections.newSetFromMap(new IdentityHashMap<Service, Boolean>());
      for (Set<Service> registeredServices : services.values()) {
        for (Service service : registeredServices) {
          if (stoppedServices.contains(service)) {
            continue;
          }
          try {
            service.stop();
          } catch (Exception e) {
            if (firstException == null) {
              firstException = e;
            } else {
              LOGGER.error("Stopping Service failed due to ", e);
            }
          }
          stoppedServices.add(service);
        }
      }
    } finally {
      lock.unlock();
    }
    if(firstException != null) {
      throw firstException;
    }
  }

  /**
   * Ensures the dependencies, as declared using the {@link ServiceDependencies} annotation,
   * of the specified class are registered in this {@code ServiceLocator}.  If a dependency
   * is not registered when this method is invoked, an attempt to load it will be made using
   * the {@link ServiceFactory} infrastructure.
   *
   * @param clazz the class for which dependency availability is checked
   */
  public void loadDependenciesOf(Class<?> clazz) {
    final Collection<Class<?>> transitiveDependencies = identifyTransitiveDependenciesOf(clazz);
    for (Class aClass : transitiveDependencies) {
      if (findServices(aClass, null, true).isEmpty()) {
        throw new IllegalStateException("Unable to resolve dependent service: " + aClass.getName());
      }
    }
  }

  /**
   * Identifies, transitively, all dependencies declared for the designated class through
   * {@link ServiceDependencies} annotations.  This method intentionally accepts
   * {@code ServiceDependencies} annotations on non-{@code Service} implementations to
   * permit classes like cache manager implementations to declare dependencies on
   * services.  All types referred to by the {@code ServiceDependencies} annotation
   * <b>must</b> be subtypes of {@link Service}.
   *
   * @param clazz the top-level class instance for which the dependencies are to be determined
   *
   * @return the collection of declared dependencies
   *
   * @see #identifyTransitiveDependenciesOf(Class, Set)
   */
  // Package-private for unit tests
  Collection<Class<?>> identifyTransitiveDependenciesOf(final Class<?> clazz) {
    return identifyTransitiveDependenciesOf(clazz, new LinkedHashSet<Class<?>>());
  }

  /**
   * Identifies the transitive dependencies of the designated class as declared through
   * {@link ServiceDependencies} annotations.
   *
   * @param clazz the class to check for declared dependencies
   * @param dependencies the current set of declared dependencies; this set will be added updated
   *
   * @return the set {@code dependencies}
   *
   * @see #identifyTransitiveDependenciesOf(Class)
   */
  private Collection<Class<?>> identifyTransitiveDependenciesOf(final Class<?> clazz, final Set<Class<?>> dependencies) {
    if (clazz == null || clazz == Object.class) {
      return dependencies;
    }

    final ServiceDependencies annotation = clazz.getAnnotation(ServiceDependencies.class);
    if (annotation != null) {
      for (final Class<?> dependency : annotation.value()) {
        if (!dependencies.contains(dependency)) {
          if (!Service.class.isAssignableFrom(dependency)) {
            throw new IllegalStateException("Service dependency declared by " + clazz.getName() +
                " is not a Service: " + dependency.getName());
          }
          dependencies.add(dependency);
          identifyTransitiveDependenciesOf(dependency, dependencies);
        }
      }
    }

    for (Class<?> interfaceClazz : clazz.getInterfaces()) {
      if (Service.class != interfaceClazz && Service.class.isAssignableFrom(interfaceClazz)) {
        identifyTransitiveDependenciesOf(interfaceClazz, dependencies);
      }
    }

    identifyTransitiveDependenciesOf(clazz.getSuperclass(), dependencies);

    return dependencies;
  }

  public boolean knowsServiceFor(ServiceConfiguration serviceConfig) {
    return !getServicesOfType(serviceConfig.getServiceType()).isEmpty();
  }

  @Override
  public <T extends Service> Collection<T> getServicesOfType(Class<T> serviceType) {
    return getServicesOfTypeInternal(serviceType);
  }

  /**
   * Gets the collection of services implementing the type specified.
   *
   * @param serviceType the subtype of {@code Service} to return
   * @param <T> the {@code Service} subtype
   *
   * @return a collection, possibly empty, of the registered services implementing {@code serviceType}
   */
  private <T extends Service> Collection<T> getServicesOfTypeInternal(final Class<T> serviceType) {
    HashSet<T> result = new LinkedHashSet<T>();
    final Set<Service> registeredServices = this.services.get(serviceType);
    if (registeredServices != null) {
      for (Service service : registeredServices) {
        result.add(serviceType.cast(service));
      }
    }
    return result;
  }
}
