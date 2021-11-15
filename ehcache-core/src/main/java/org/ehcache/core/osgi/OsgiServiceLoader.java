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

package org.ehcache.core.osgi;

import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * An OSGi service based equivalent to {@link java.util.ServiceLoader}.
 * <p>
 * This class is used by the {@link org.ehcache.core.spi.ServiceLocator ServiceLocator} (via
 * {@link org.ehcache.core.util.ClassLoading#servicesOfType(Class) ClassLoading.servicesOfType(Class)}) to discover services when running inside an OSGi
 * environment. This is needed when the required Ehcache services are split across multiple OSGi bundles.
 */
public class OsgiServiceLoader {

  /**
   * Locate all services of type {@code T}.
   *
   * @param serviceType concrete service class
   * @param <T> service type
   * @return an iterable of {@code T} services
   */
  public static <T> Iterable<T> load(Class<T> serviceType) {
    try {
      BundleContext coreBundle = EhcacheActivator.getCoreBundle();
      return coreBundle.getServiceReferences(serviceType, null).stream().map(coreBundle::getService).collect(toList());
    } catch (InvalidSyntaxException e) {
      throw new AssertionError(e);
    }
  }
}
