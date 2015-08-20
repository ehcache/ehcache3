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

import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceFactory;
import org.ehcache.spi.service.SupplementaryService;
import org.ehcache.util.ClassLoading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
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
 * @author Alex Snaps
 */
public final class ServiceLocator implements ServiceProvider {

  
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceLocator.class);
  private final ConcurrentMap<Class<? extends Service>, Service> services = new ConcurrentHashMap<Class<? extends Service>, Service>();
  
  @SuppressWarnings("rawtypes")
  private final ServiceLoader<ServiceFactory> serviceFactory = ClassLoading.libraryServiceLoaderFor(ServiceFactory.class);

  private final ReadWriteLock runningLock = new ReentrantReadWriteLock();

  private final AtomicBoolean running = new AtomicBoolean(false);

  public ServiceLocator(Service... services) {
    for (Service service : services) {
      addService(service);
    }
  }

  private <T extends Service> T discoverService(Class<T> serviceClass, ServiceCreationConfiguration<T> config) {
    for (ServiceFactory<T> factory : ServiceLocator.<T> getServiceFactories(serviceFactory)) {
      if (serviceClass.isAssignableFrom(factory.getServiceType())) {
        T service = factory.create(config);
        addService(service, true);
        return service;
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static <T extends Service> Iterable<ServiceFactory<T>> getServiceFactories(@SuppressWarnings("rawtypes") ServiceLoader<ServiceFactory> serviceFactory) {
    List<ServiceFactory<T>> list = new ArrayList<ServiceFactory<T>>();
    for (ServiceFactory<?> factory : serviceFactory) {
      list.add((ServiceFactory<T>)factory);
    }
    return list;
  }

  public void addService(final Service service) {
    addService(service, false);
  }

  void addService(final Service service, final boolean expectsAbstractRegistration) {
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

      if (services.putIfAbsent(service.getClass(), service) != null) {
        throw new IllegalStateException("Registration of duplicate service " + service.getClass());
      }

      if (!service.getClass().isAnnotationPresent(SupplementaryService.class)) {
        boolean registered = false;
        for (Class<? extends Service> serviceClazz : serviceClazzes) {
          if (services.putIfAbsent(serviceClazz, service) == null && !registered) {
            registered = true;
          }
        }
        if (!registered) {
          final StringBuilder message = new StringBuilder("Duplicate service implementation found for ").append(serviceClazzes)
              .append(" by ")
              .append(service.getClass());
          for (Class<? extends Service> serviceClass : serviceClazzes) {
            final Service declaredService = services.get(serviceClass);
            if (declaredService != null) {
              message
                  .append("\n\t\t- ")
                  .append(serviceClass)
                  .append(" already has ")
                  .append(declaredService.getClass());
            }
          }
          if (expectsAbstractRegistration) {
            throw new IllegalStateException(message.toString());
          }
          LOGGER.debug(message.toString());
        }
      }

      loadDependenciesOf(service.getClass());

      if (running.get()) {
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

  public <T extends Service> T getOrCreateServiceFor(ServiceCreationConfiguration<T> config) {
    return findService(config.getServiceType(), config, true);
  }

  @Override
  public <T extends Service> T getService(Class<T> serviceType) {
    return findService(serviceType, null, false);
  }

  private <T extends Service> T findService(Class<T> serviceType, ServiceCreationConfiguration<T> config, boolean shouldCreate) {
    T service = serviceType.cast(services.get(serviceType));
    if (service == null && shouldCreate) {
      return discoverService(serviceType, config);
    } else {
      return service;
    }
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

  public static <T> T findSingletonAmongst(Class<T> clazz, Object ... instances) {
    final Collection<T> matches = findAmongst(clazz, instances);
    if (matches.isEmpty()) {
      return null;
    } else if (matches.size() == 1) {
      return matches.iterator().next();
    } else {
      throw new IllegalArgumentException();
    }
  }

  public void startAllServices() throws Exception {
    Deque<Service> started = new ArrayDeque<Service>();
    final Lock lock = runningLock.writeLock();
    lock.lock();
    try {
      if (!running.compareAndSet(false, true)) {
        throw new IllegalStateException("Already started!");
      }
      for (Service service : services.values()) {
        if (!started.contains(service)) {
          service.start(this);
          started.push(service);
        }
      }
      LOGGER.info("All Services successfully started.");
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
      Set<Service> stoppedServices = Collections.newSetFromMap(new IdentityHashMap<Service, Boolean>());
      for (Service service : services.values()) {
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
    } finally {
      lock.unlock();
    }
    if(firstException != null) {
      throw firstException;
    }
  }

  public void loadDependenciesOf(Class<?> clazz) {
    ServiceDependencies annotation = clazz.getAnnotation(ServiceDependencies.class);
    if (annotation != null) {
      for (Class aClass : annotation.value()) {
        if (findService(aClass, null, true) == null) {
          throw new IllegalStateException("Unable to resolve dependent service: " + aClass.getSimpleName());
        }
      }
    }
  }
}
