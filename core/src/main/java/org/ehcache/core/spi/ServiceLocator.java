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

import org.ehcache.config.Builder;
import org.ehcache.spi.service.OptionalServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.core.spi.service.ServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.ehcache.core.util.ClassLoading.delegationChain;
import static org.ehcache.core.util.ClassLoading.getDefaultClassLoader;
import static org.ehcache.core.util.ClassLoading.servicesOfType;

/**
 * Provides discovery and tracking services for {@link Service} implementations.
 */
public final class ServiceLocator implements ServiceProvider<Service> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceLocator.class);
  private final ServiceMap services;

  private final ReadWriteLock runningLock = new ReentrantReadWriteLock();

  private final AtomicBoolean running = new AtomicBoolean(false);

  public static DependencySet dependencySet() {
    return new DependencySet();
  }

  private ServiceLocator(ServiceMap services) {
    this.services = services;
  }

  @Override
  public <T extends Service> T getService(Class<T> serviceType) {
    if (serviceType.isAnnotationPresent(PluralService.class)) {
      throw new IllegalArgumentException(serviceType.getName() + " is marked as a PluralService");
    }
    final Collection<T> registeredServices = getServicesOfType(serviceType);
    if (registeredServices.size() > 1) {
      throw new AssertionError("The non-PluralService type" + serviceType.getName()
          + " has more than one service registered");
    }
    return (registeredServices.isEmpty() ? null : registeredServices.iterator().next());
  }

  @Override
  public <T extends Service> Collection<T> getServicesOfType(Class<T> serviceType) {
    return services.get(serviceType);
  }

  public boolean knowsServiceFor(ServiceConfiguration<?, ?> serviceConfig) {
    return services.contains(serviceConfig.getServiceType());
  }

  public void startAllServices() {
    Deque<Service> started = new LinkedList<>();
    final Lock lock = runningLock.writeLock();
    lock.lock();
    try {
      if (!running.compareAndSet(false, true)) {
        throw new IllegalStateException("Already started!");
      }

      /*
       * This ensures that we start services in dependency order
       */
      LinkedList<Service> unstarted = new LinkedList<>(services.all());
      int totalServices = unstarted.size();
      long start = System.currentTimeMillis();
      LOGGER.debug("Starting {} Services...", totalServices);
      while (!unstarted.isEmpty()) {
        boolean startedSomething = false;
        for (Iterator<Service> it = unstarted.iterator(); it.hasNext(); ) {
          Service s = it.next();
          if (hasUnstartedDependencies(s, unstarted)) {
            LOGGER.trace("Delaying starting {}", s);
          } else {
            LOGGER.trace("Starting {}", s);
            s.start(this);
            started.push(s);
            it.remove();
            startedSomething = true;
          }
        }
        if (startedSomething) {
          LOGGER.trace("Cycle complete: " + unstarted.size() + " Services remaining");
        } else {
          throw new IllegalStateException("Cyclic dependency in Service set: " + unstarted);
        }
      }
      LOGGER.debug("All Services successfully started, {} Services in {}ms", totalServices, System.currentTimeMillis() - start);
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

  public void stopAllServices() throws Exception {
    Exception firstException = null;
    Lock lock = runningLock.writeLock();
    lock.lock();
    try {
      if(!running.compareAndSet(true, false)) {
        throw new IllegalStateException("Already stopped!");
      }

      /*
       * This ensures that we stop services in dependency order
       */
      Collection<Service> running = new LinkedList<>(services.all());
      int totalServices = running.size();
      long start = System.currentTimeMillis();
      LOGGER.debug("Stopping {} Services...", totalServices);
      while (!running.isEmpty()) {
        boolean stoppedSomething = false;
        for (Iterator<Service> it = running.iterator(); it.hasNext(); ) {
          Service s = it.next();
          if (hasRunningDependents(s, running)) {
            LOGGER.trace("Delaying stopping {}", s);
          } else {
            LOGGER.trace("Stopping {}", s);
            try {
              s.stop();
            } catch (Exception e) {
              if (firstException == null) {
                firstException = e;
              } else {
                LOGGER.error("Stopping Service failed due to ", e);
              }
            }
            it.remove();
            stoppedSomething = true;
          }
        }
        if (stoppedSomething) {
          LOGGER.trace("Cycle complete: " + running.size() + " Services remaining");
        } else {
          throw new AssertionError("Cyclic dependency in Service set: " + running);
        }
      }
      LOGGER.debug("All Services successfully stopped, {} Services in {}ms", totalServices, System.currentTimeMillis() - start);
    } finally {
      lock.unlock();
    }
    if(firstException != null) {
      throw firstException;
    }
  }

  private boolean hasUnstartedDependencies(Service service, Iterable<Service> unstarted) {
    for (Class<? extends Service> dep : identifyTransitiveDependenciesOf(service.getClass())) {
      for (Service s : unstarted) {
        if (dep.isInstance(s)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean hasRunningDependents(Service service, Iterable<Service> running) {
    for (Service runningService : running) {
      Set<Class<? extends Service>> dependencyClasses = identifyTransitiveDependenciesOf(runningService.getClass());
      for (Class<? extends Service> dependencyClass : dependencyClasses) {
        if (dependencyClass.isInstance(service)) {
          return true;
        }
      }
    }
    return false;
  }

  public static class DependencySet implements Builder<ServiceLocator> {

    @SuppressWarnings({"rawtypes", "unchecked"})
    private final Iterable<ServiceFactory<?>> serviceFactories = (Iterable) servicesOfType(ServiceFactory.class);

    private final ServiceMap provided = new ServiceMap();
    private final Set<Class<? extends Service>> requested = new HashSet<>();
    private boolean includeMandatoryServices = true;

    public DependencySet with(Service service) {
      provided.add(service);
      return this;
    }

    public DependencySet with(Iterable<? extends Service> services) {
      for (Service s : services) {
        with(s);
      }
      return this;
    }

    public <T extends Service> DependencySet with(ServiceCreationConfiguration<T, ?> config) {
      Class<T> serviceType = config.getServiceType();

      //TODO : This stanza is due to the way we use configure the JSR-107 service
      if (provided.contains(serviceType) && !serviceType.isAnnotationPresent(PluralService.class)) {
        return this;
      }

      @SuppressWarnings("unchecked")
      Collection<ServiceFactory<T>> typedServiceFactories = stream(serviceFactories.spliterator(), false)
        .filter(f -> serviceType.isAssignableFrom(f.getServiceType())).map(f -> (ServiceFactory<T>) f)
        .collect(toList());

      OptionalInt highestRank = typedServiceFactories.stream().mapToInt(ServiceFactory::rank).max();

      if (highestRank.isPresent()) {
        typedServiceFactories.stream().filter(f -> highestRank.getAsInt() == f.rank()).forEach(f -> with(f.create(config)));
        return this;
      } else {
        throw new IllegalStateException("No factories exist for " + serviceType);
      }
    }

    public DependencySet with(Class<? extends Service> clazz) {
      requested.add(clazz);
      return this;
    }


    public DependencySet withoutMandatoryServices() {
      includeMandatoryServices = false;
      return this;
    }

    public boolean contains(Class<? extends Service> serviceClass) {
      return provided.contains(serviceClass);
    }

    public <T extends Service> T providerOf(Class<T> serviceClass) {
      if (serviceClass.isAnnotationPresent(PluralService.class)) {
        throw new IllegalArgumentException("Cannot retrieve single provider for plural service");
      } else {
        Collection<T> providers = providersOf(serviceClass);
        switch (providers.size()) {
          case 0:
            return null;
          case 1:
            return providers.iterator().next();
          default:
            throw new AssertionError();
        }
      }
    }

    public <T extends Service> Collection<T> providersOf(Class<T> serviceClass) {
      return provided.get(serviceClass);
    }

    @Override
    public ServiceLocator build() {
      try {
        ServiceMap resolvedServices = new ServiceMap();

        for (Service service : provided.all()) {
          resolvedServices = lookupDependenciesOf(resolvedServices, service.getClass()).add(service);
        }

        for (Class<? extends Service> request : requested) {
          if (request.isAnnotationPresent(PluralService.class)) {
            try {
              resolvedServices = lookupService(resolvedServices, request);
            } catch (DependencyException e) {
              if (!resolvedServices.contains(request)) {
                throw e;
              }
            }
          } else if (!resolvedServices.contains(request)) {
            resolvedServices = lookupService(resolvedServices, request);
          }
        }

        if (includeMandatoryServices) {
          for (List<ServiceFactory<?>> factories : stream(serviceFactories.spliterator(), false).collect(groupingBy(ServiceFactory::getServiceType)).values()) {
            OptionalInt highestRank = factories.stream().mapToInt(ServiceFactory::rank).max();

            if (highestRank.isPresent()) {
              for (ServiceFactory<?> manadatory : factories.stream().filter(ServiceFactory::isMandatory).filter(f -> highestRank.getAsInt() == f.rank()).collect(toList())) {
                if (!resolvedServices.contains(manadatory.getServiceType())) {
                  Service service = manadatory.create(null);
                  resolvedServices = lookupDependenciesOf(resolvedServices, service.getClass()).add(service);
                }
              }
            }
          }
        }

        return new ServiceLocator(resolvedServices);
      } catch (DependencyException e) {
        throw new IllegalStateException(e);
      }
    }

    ServiceMap lookupDependenciesOf(ServiceMap resolved, Class<? extends Service> requested) throws DependencyException {
      for (Class<? extends Service> dependency : identifyImmediateDependenciesOf(requested)) {
        try {
          resolved = lookupService(resolved, dependency);
        } catch (DependencyException de) {
          OptionalServiceDependencies optionalAnnotation = requested.getAnnotation(OptionalServiceDependencies.class);
          if (optionalAnnotation != null && Arrays.asList(optionalAnnotation.value()).contains(dependency.getName())) {
            LOGGER.debug("Skipping optional dependency of {} that cannot be looked up: {}", requested, dependency);
            continue;
          }
          throw de;
        }
      }
      return resolved;
    }

    private <T extends Service> ServiceMap lookupService(ServiceMap resolved, Class<T> requested) throws DependencyException {
      //Have we already resolved this dependency?
      if (resolved.contains(requested) && !requested.isAnnotationPresent(PluralService.class)) {
        return resolved;
      }
      //Attempt resolution from the provided services
      resolved = new ServiceMap(resolved).addAll(provided.get(requested));
      if (resolved.contains(requested) && !requested.isAnnotationPresent(PluralService.class)) {
        return resolved;
      }
      Collection<ServiceFactory<? extends T>> serviceFactories = discoverServices(resolved, requested);
      if (serviceFactories.size() > 1 && !requested.isAnnotationPresent(PluralService.class)) {
        throw new DependencyException("Multiple factories for non-plural service");
      }
      for(ServiceFactory<? extends T> factory : serviceFactories) {
        if (!resolved.contains(factory.getServiceType())) {
          try {
            resolved = lookupDependenciesOf(resolved, factory.getServiceType());
          } catch (DependencyException e) {
            continue;
          }

          T service = factory.create(null);

          //we copy the service map so that if upstream dependency resolution fails we don't pollute the real resolved set
          resolved = new ServiceMap(resolved).add(service);
        }
      }
      if (resolved.contains(requested)) {
        return resolved;
      } else {
        throw new DependencyException("Failed to find provider with satisfied dependency set for " + requested + " [candidates " + serviceFactories + "]");
      }
    }

    /**
     * For the {@link Service} class specified, attempt to instantiate the service using the
     * {@link ServiceFactory} infrastructure.
     *
     * @param serviceClass the {@code Service} type to create
     * @param <T> the type of the {@code Service}
     *
     * @return the collection of created services; may be empty
     *
     * @throws IllegalStateException if the configured service is already registered or the configured service
     *        implements a {@code Service} subtype that is not marked with the {@link PluralService} annotation
     *        but is already registered
     */
    private <T, V> Collection<ServiceFactory<? extends T>> discoverServices(ServiceMap resolved, Class<T> serviceClass) {
      @SuppressWarnings("unchecked")
      Collection<ServiceFactory<? extends T>> typedServiceFactories = stream(serviceFactories.spliterator(), false)
        .filter(f -> serviceClass.isAssignableFrom(f.getServiceType())).map(f -> (ServiceFactory<? extends T>) f)
        .filter(f -> !f.getClass().isAnnotationPresent(ServiceFactory.RequiresConfiguration.class))
        .filter(f -> !provided.contains(f.getServiceType()))
        .filter(f -> !resolved.contains(f.getServiceType()))
        .collect(toList());

      OptionalInt highestRank = typedServiceFactories.stream().mapToInt(ServiceFactory::rank).max();

      if (highestRank.isPresent()) {
        return typedServiceFactories.stream().filter(f -> highestRank.getAsInt() == f.rank()).collect(toList());
      } else {
        return emptyList();
      }
    }
  }

  private static Collection<Class<?>> getAllInterfaces(final Class<?> clazz) {
    ArrayList<Class<?>> interfaces = new ArrayList<>();
    for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
      for (Class<?> i : c.getInterfaces()) {
        interfaces.add(i);
        interfaces.addAll(getAllInterfaces(i));
      }
    }
    return interfaces;
  }

  private static Set<Class<? extends Service>> identifyImmediateDependenciesOf(final Class<?> clazz) {
    if (clazz == null) {
      return emptySet();
    }

    Set<Class<? extends Service>> dependencies = new HashSet<>();
    ServiceDependencies annotation = clazz.getAnnotation(ServiceDependencies.class);
    if (annotation != null) {
      for (final Class<?> dependency : annotation.value()) {
        if (Service.class.isAssignableFrom(dependency)) {
          @SuppressWarnings("unchecked")
          Class<? extends Service> serviceDependency = (Class<? extends Service>) dependency;
          dependencies.add(serviceDependency);
        } else {
          throw new IllegalStateException("Service dependency declared by " + clazz.getName() +
            " is not a Service: " + dependency.getName());
        }
      }
    }
    OptionalServiceDependencies optionalAnnotation = clazz.getAnnotation(OptionalServiceDependencies.class);
    if (optionalAnnotation != null) {
      for (String className : optionalAnnotation.value()) {
        try {
          Class<?> dependencyClass = delegationChain(getDefaultClassLoader(), clazz.getClassLoader()).loadClass(className);
          if (Service.class.isAssignableFrom(dependencyClass)) {
            @SuppressWarnings("unchecked")
            Class<? extends Service> serviceDependency = (Class<? extends Service>) dependencyClass;
            dependencies.add(serviceDependency);
          } else {
            throw new IllegalStateException("Service dependency declared by " + className + " is not a Service: " + dependencyClass.getName());
          }
        } catch (ClassNotFoundException ignored) {
          // dependency is optional so we ignore it
        }
      }
    }

    for (Class<?> interfaceClazz : clazz.getInterfaces()) {
      if (Service.class.isAssignableFrom(interfaceClazz)) {
        dependencies.addAll(identifyImmediateDependenciesOf(Service.class.getClass().cast(interfaceClazz)));
      }
    }

    dependencies.addAll(identifyImmediateDependenciesOf(clazz.getSuperclass()));

    return dependencies;
  }

  private static Set<Class<? extends Service>> identifyTransitiveDependenciesOf(final Class<?> clazz) {

    Set<Class<? extends Service>> dependencies = identifyImmediateDependenciesOf(clazz);
    for (Class<? extends Service> dependencyClass : dependencies) {
      if (dependencyClass == clazz) {
        throw new IllegalStateException("Circular dependency found. Service " + clazz.getName() + " cannot depend on itself.");
      }
    }
    Set<Class<? extends Service>> transitive = new HashSet<>(dependencies.size() * 3); // 3 is my feeling of how many there should be per class at most
    transitive.addAll(dependencies);

    for (Class<? extends Service> klazz : dependencies) {
      Set<Class<? extends Service>> identified = identifyTransitiveDependenciesOf(klazz);
      for (Class<? extends Service> dep : identified) {
        if(dep == clazz) {
          throw new IllegalStateException("Circular dependency found. A dependency of service " + clazz.getName() + " depends on it.");
        }
      }
      transitive.addAll(identified);
    }

    return transitive;
  }

  private static class DependencyException extends Exception {
    private static final long serialVersionUID = -5269926129639323941L;

    public DependencyException(String s) {
      super(s);
    }
  }

  private static class ServiceMap {

    private final Map<Class<? extends Service>, Set<Service>> services;

    public ServiceMap(ServiceMap resolved) {
      this.services = new HashMap<>();
      for (Map.Entry<Class<? extends Service>, Set<Service>> e : resolved.services.entrySet()) {
        Set<Service> copy = newSetFromMap(new IdentityHashMap<Service, Boolean>());
        copy.addAll(e.getValue());
        this.services.put(e.getKey(), copy);
      }
    }

    public ServiceMap() {
      this.services = new HashMap<>();
    }

    public <T extends Service> Set<T> get(Class<T> serviceType) {
      @SuppressWarnings("unchecked")
      Set<T> s = (Set<T>) services.get(serviceType);
      if (s == null) {
        return emptySet();
      } else {
        return unmodifiableSet(s);
      }
    }

    public ServiceMap addAll(Iterable<? extends Service> services) {
      for (Service s : services) {
        add(s);
      }
      return this;
    }

    public ServiceMap add(Service service) {
      Set<Class<? extends Service>> serviceClazzes = new HashSet<>();

      serviceClazzes.add(service.getClass());
      for (Class<?> i : getAllInterfaces(service.getClass())) {
        if (Service.class != i && Service.class.isAssignableFrom(i)) {

          @SuppressWarnings("unchecked")
          Class<? extends Service> serviceClass = (Class<? extends Service>) i;

          serviceClazzes.add(serviceClass);
        }
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
            registeredServices = new LinkedHashSet<>();
            services.put(serviceClazz, registeredServices);
          }
          registeredServices.add(service);
        } else {
          // Only a single registration permitted
          Set<Service> registeredServices = services.get(serviceClazz);
          if (registeredServices == null || registeredServices.isEmpty()) {
            services.put(serviceClazz, singleton(service));
          } else if (!registeredServices.contains(service)) {
            final StringBuilder message = new StringBuilder("Duplicate service implementation(s) found for ")
              .append(service.getClass());
            for (Class<? extends Service> serviceClass : serviceClazzes) {
              if (!serviceClass.isAnnotationPresent(PluralService.class)) {
                Set<Service> s = this.services.get(serviceClass);
                final Service declaredService = s == null ? null : s.iterator().next();
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
      return this;
    }

    public Set<Service> all() {
      Set<Service> all = newSetFromMap(new IdentityHashMap<Service, Boolean>());
      for (Set<Service> s : services.values()) {
        all.addAll(s);
      }
      return unmodifiableSet(all);
    }

    public boolean contains(Class<? extends Service> request) {
      return services.containsKey(request);
    }
  }
}
