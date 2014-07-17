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

import org.ehcache.internal.ServiceLocator;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Alex Snaps
 */
public final class ServiceProvider implements ServiceLocator {

  private final ConcurrentMap<Class<? extends Service>, Service> services = new ConcurrentHashMap<Class<? extends Service>, Service>();
  private final ServiceLoader<ServiceFactory> serviceFactory = ServiceLoader.load(ServiceFactory.class);

  private final ReadWriteLock runningLock = new ReentrantReadWriteLock();
  private boolean running = false;
  
  public ServiceProvider(Service ... services) {
    for (Service service : services) {
      addService(service);
    }
  }

  public <T extends Service> T discoverService(Class<T> serviceClass) {
    return discoverService(serviceClass, null);
  }

  public <T extends Service> T discoverService(Class<T> serviceClass, ServiceConfiguration<T> config) {
    // TODO Fix me!
    for (ServiceFactory<T> factory : serviceFactory) {
      if (serviceClass.isAssignableFrom(factory.getServiceType())) {
        T service = factory.create(config);
        addService(service);
        return service;
      }
    }
    return null;
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
        if(Service.class != i && Service.class.isAssignableFrom(i)) {
          serviceClazzes.add((Class<? extends Service>) i);
        }
      }

      if (serviceClazzes.isEmpty()) {
        throw new IllegalArgumentException("Service implements no service interfaces.");
      }

      /*
      * Also deal with races here
      */
      if (running) {
        boolean interrupted = false;
        try {
          while (true) {
            try {
              service.start().get();
            } catch (InterruptedException ex) {
              interrupted = true;
            } catch (ExecutionException ex) {
              Throwable t = ex.getCause();
              if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
              } else if (t instanceof Error) {
                throw (Error) t;
              } else {
                throw new RuntimeException(t);
              }
            }
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }

      HashSet<Class<?>> existingServices = new HashSet<Class<?>>(serviceClazzes);
      existingServices.retainAll(services.keySet());
      if (existingServices.isEmpty()) {
        for (Class<? extends Service> serviceClazz : serviceClazzes) {
          if (services.putIfAbsent(serviceClazz, service) != null) {
            throw new IllegalStateException("Racing registration for duplicate service " + serviceClazz.getName());
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
    T service = (T) services.get(serviceType);
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
        matches.add((T)instance);
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

  public void startAllServices() throws InterruptedException {
    Lock lock = runningLock.writeLock();
    lock.lock();
    try {
      Collection<Future<?>> starts = new ArrayList<Future<?>>();
      for (Service service : services.values()) {
        starts.add(service.start());
      }

      for (Future<?> start : starts) {
        try {
          start.get();
        } catch (ExecutionException ex) {
          Throwable t = ex.getCause();
          if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
          } else if (t instanceof Error) {
            throw (Error) t;
          } else {
            throw new RuntimeException(t);
          }
        }
      }    
      running = true;
    } finally {
      lock.unlock();
    }
  }
  
  public void stopAllServices() throws InterruptedException {
    Lock lock = runningLock.writeLock();
    lock.lock();
    try {
      Collection<Future<?>> stops = new ArrayList<Future<?>>();
      for (Service service : services.values()) {
        stops.add(service.stop());
      }

      for (Future<?> stop : stops) {
        try {
          stop.get();
        } catch (ExecutionException ex) {
          Throwable t = ex.getCause();
          if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
          } else if (t instanceof Error) {
            throw (Error) t;
          } else {
            throw new RuntimeException(t);
          }
        }
      }
      running = false;
    } finally {
      lock.unlock();
    }
  }
}