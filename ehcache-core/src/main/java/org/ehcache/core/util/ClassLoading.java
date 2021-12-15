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

package org.ehcache.core.util;

import org.ehcache.core.osgi.SafeOsgi;
import org.ehcache.core.osgi.OsgiServiceLoader;

import java.io.IOException;
import java.net.URL;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Supplier;

import static java.security.AccessController.doPrivileged;
import static java.util.Collections.enumeration;
import static java.util.Collections.list;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;

public class ClassLoading {

  private static final ClassLoader DEFAULT_CLASSLOADER;

  static {
    DEFAULT_CLASSLOADER = delegationChain(() -> Thread.currentThread().getContextClassLoader(), ChainedClassLoader.class.getClassLoader());
  }

  public static ClassLoader getDefaultClassLoader() {
    return DEFAULT_CLASSLOADER;
  }

  public static <T> Iterable<T> servicesOfType(Class<T> serviceType) {
    if (SafeOsgi.useOSGiServiceLoading()) {
      return OsgiServiceLoader.load(serviceType);
    } else {
      return ServiceLoader.load(serviceType, ClassLoading.class.getClassLoader());
    }
  }

  @SuppressWarnings("unchecked")
  public static ClassLoader delegationChain(Supplier<ClassLoader> loader, ClassLoader ... loaders) {
    return doPrivileged((PrivilegedAction<ClassLoader>) () -> new ChainedClassLoader(concat(of(loader), of(loaders).map(l -> () -> l)).collect(toList())));
  }

  @SuppressWarnings("unchecked")
  public static ClassLoader delegationChain(ClassLoader ... loaders) {
    return doPrivileged((PrivilegedAction<ClassLoader>) () -> new ChainedClassLoader(of(loaders).<Supplier<ClassLoader>>map(l -> () -> l).collect(toList())));
  }

  private static class ChainedClassLoader extends ClassLoader {

    private final List<Supplier<ClassLoader>> loaders;

    public ChainedClassLoader(List<Supplier<ClassLoader>> loaders) {
      this.loaders = loaders;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      ClassNotFoundException lastFailure = new ClassNotFoundException(name);
      for (Supplier<ClassLoader> loader : loaders) {
        ClassLoader classLoader = loader.get();
        if (classLoader != null) {
          try {
            return classLoader.loadClass(name);
          } catch (ClassNotFoundException cnfe) {
            lastFailure = cnfe;
          }
        }
      }
      throw lastFailure;
    }

    @Override
    public URL getResource(String name) {
      for (Supplier<ClassLoader> loader : loaders) {
        ClassLoader classLoader = loader.get();
        if (classLoader != null) {
          URL resource = classLoader.getResource(name);
          if (resource != null) {
            return resource;
          }
        }
      }
      return null;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
      Collection<URL> aggregate = new ArrayList<>();
      for (Supplier<ClassLoader> loader : loaders) {
        ClassLoader classLoader = loader.get();
        if (classLoader != null) {
          aggregate.addAll(list(classLoader.getResources(name)));
        }
      }
      return enumeration(aggregate);
    }
  }
}
