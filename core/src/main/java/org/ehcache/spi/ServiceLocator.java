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
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceFactory;
import org.ehcache.util.ClassLoading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
public final class ServiceLocator {

  
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

  public <T extends Service> T discoverService(Class<T> serviceClass) {
    return discoverService(serviceClass, null);
  }

  public <T extends Service> T discoverService(Class<T> serviceClass, ServiceConfiguration<T> config) {
    // TODO Fix me!
    for (ServiceFactory<T> factory : ServiceLocator.<T> getServiceFactories(serviceFactory)) {
      if (serviceClass.isAssignableFrom(factory.getServiceType())) {
        T service = factory.create(config, this);
        addService(service);
        return service;
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static <T extends Service> Iterable<ServiceFactory<T>> getServiceFactories(@SuppressWarnings("rawtypes") ServiceLoader<ServiceFactory> serviceFactory) {
    List<ServiceFactory<T>> list = new ArrayList<ServiceFactory<T>>();
    for (ServiceFactory<?> factory : serviceFactory) {
      list.add((ServiceFactory<T>) factory);
    }
    return list;
  }

  public <T extends Service> T discoverService(ServiceConfiguration<T> config) {
    return discoverService(config.getServiceType(), config);
  }
  
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

      if (serviceClazzes.isEmpty()) {
        throw new IllegalArgumentException("Service implements no service interfaces.");
      }

      HashSet<Class<?>> existingServices = new HashSet<Class<?>>(serviceClazzes);
      existingServices.retainAll(services.keySet());
      if (existingServices.isEmpty()) {
        for (Class<? extends Service> serviceClazz : serviceClazzes) {
          if (services.putIfAbsent(serviceClazz, service) != null) {
            throw new IllegalStateException("Racing registration for duplicate service " + serviceClazz.getName());
          } else if (running.get()) {
            service.start(null);
          }
        }
      } else {
        throw new IllegalStateException("Already have services registered for " + existingServices);
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

  public <T extends Service> T findService(Class<T> serviceType) {
    return findService(serviceType, null);
  }

  public <T extends Service> T findService(Class<T> serviceType, ServiceConfiguration<T> config) {
    T service = serviceType.cast(services.get(serviceType));
    if (service == null) {
      return discoverService(serviceType, config);
    } else {
      return service;
    }
  }
  
  public static <T> Collection<T> findAmongst(Class<T> clazz, Object ... instances) {
    Collection<T> matches = new ArrayList<T>();
    for (Object instance : instances) {
      if(clazz.isAssignableFrom(instance.getClass())) {
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

  public void startAllServices(final Map<Service, ServiceConfiguration<?>> serviceConfigs) throws Exception {
    Deque<Service> started = new ArrayDeque<Service>();
    final Lock lock = runningLock.writeLock();
    lock.lock();
    try {
      if (!running.compareAndSet(false, true)) {
        throw new IllegalStateException("Already started!");
      }
      for (Service service : services.values()) {
        service.start(serviceConfigs.get(service));
        started.push(service);        
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
      for (Service service : services.values()) {
        try {
          service.stop();
        } catch (Exception e) {
          if (firstException == null) {
            firstException = e;
          } else {
            LOGGER.error("Stopping Service failed due to ", e);
          }
        }
      }
    } finally {
      lock.unlock();
    }
    if(firstException != null) {
      throw firstException;
    }
  }
}
