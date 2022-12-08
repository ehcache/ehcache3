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

import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.core.util.ClassLoading;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.Spliterators.spliterator;
import static java.util.stream.Collectors.joining;
import static java.util.stream.StreamSupport.stream;

public class EhcacheActivator implements BundleActivator {

  public static final String OSGI_LOADING = "org.ehcache.core.osgi";

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheActivator.class);

  private static final AtomicReference<BundleContext> CORE_BUNDLE = new AtomicReference<>();

  @Override
  public void start(BundleContext context) throws Exception {
    BundleContext currentContext = CORE_BUNDLE.getAndUpdate(current -> current == null ? context : current);
    if (currentContext == null) {
      String greeting = "Detected OSGi Environment (core is in bundle: " + context.getBundle() + ")";
      if ("false".equalsIgnoreCase(context.getProperty(OSGI_LOADING))) {
        SafeOsgi.disableOSGiServiceLoading();
        LOGGER.info(greeting + ": OSGi Based Service Loading Disabled Via System/Framework Property - Extensions Outside This Bundle Will Not Be Detected");
        LOGGER.debug("JDK Service Loading Sees:\n\t" + stream(spliterator(ClassLoading.servicesOfType(ServiceFactory.class).iterator(), Long.MAX_VALUE, 0), false)
          .map(sf -> sf.getServiceType().getName()).collect(joining("\n\t")));
      } else {
        SafeOsgi.enableOSGiServiceLoading();
        LOGGER.info(greeting + ": Using OSGi Based Service Loading");
      }
    } else {
      throw new IllegalStateException("Multiple bundle instances running against the same core classes: existing bundle: " + currentContext.getBundle() + " new bundle: " + context.getBundle());
    }
  }

  @Override
  public void stop(BundleContext context) throws Exception {
    SafeOsgi.disableOSGiServiceLoading();
    CORE_BUNDLE.set(null);
  }

  public static BundleContext getCoreBundle() {
    return CORE_BUNDLE.get();
  }
}
