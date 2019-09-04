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

/**
 * A classpath-safe decoupler for the OSGi service loading status.
 * <p>
 * This class provides an OSGi class-decoupled way of checking whether OSGi service loading should be used. It is safe
 * to load and call methods on this class when OSGi classes are not present.
 */
public final class SafeOsgi {

  private static volatile boolean OSGI_SERVICE_LOADING;

  /**
   * Returns {@code true} if OSGi based service loading should be used.
   * <p>
   * A {@code true} return indicates that Ehcache is running in an OSGi environment and that the user has enabled OSGi
   * based service loading.
   *
   * @return {@code true} if OSGi service loading is enabled.
   */
  public static boolean useOSGiServiceLoading() {
    return OSGI_SERVICE_LOADING;
  }

  /**
   * Marks OSGi service loading as enabled.
   * <p>
   * This is called by the {@link EhcacheActivator} when the user has enabled OSGi service loading.
   */
  static void enableOSGiServiceLoading() {
    OSGI_SERVICE_LOADING = true;
  }

  /**
   * Marks OSGi service loading as enabled.
   * <p>
   * This is called by the {@link EhcacheActivator} when the user has not enabled OSGi service loading, and also when
   * the Ehcache core bundle is stopped.
   */
  static void disableOSGiServiceLoading() {
    OSGI_SERVICE_LOADING = false;
  }

  private SafeOsgi() {
    //static holder
  }
}
