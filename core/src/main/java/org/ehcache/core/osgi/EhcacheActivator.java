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

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

public class EhcacheActivator implements BundleActivator {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheActivator.class);

  private static final AtomicReference<BundleContext> CORE_BUNDLE = new AtomicReference<>();

  @Override
  public void start(BundleContext context) throws Exception {
    BundleContext currentContext = CORE_BUNDLE.getAndUpdate(current -> current == null ? context : current);
    if (currentContext == null) {
      SafeOsgi.enableOSGiServiceLoading();
      LOGGER.info("Detected OSGi Environment (core is in bundle: " + context.getBundle() + "): OSGi Based Service Loading Enabled Via System/Framework Property");
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
