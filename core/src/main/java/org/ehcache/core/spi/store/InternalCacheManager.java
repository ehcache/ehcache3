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

package org.ehcache.core.spi.store;


import org.ehcache.CacheManager;
import org.ehcache.core.events.CacheManagerListener;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;

/**
 * The {@code Service}-facing version of a {@code CacheManager}.  This interface adds
 * methods used internally by service implementations.
 * <p>
 * In rare cases it can be used by Ehcache power users to access things that are now exposed by the standard API. <b>Use it
 * at your own risk.</b>
 */
public interface InternalCacheManager extends CacheManager {

  /**
   * Registers a {@link CacheManagerListener}.
   *
   * @param listener the listener to register
   */
  void registerListener(CacheManagerListener listener);

  /**
   * De-registers a {@link CacheManagerListener}.
   *
   * @param listener the listener to de-register
   */
  void deregisterListener(CacheManagerListener listener);

  /**
   * Returns the {@link ServiceProvider} referencing all the services of this {@code CacheManager}. All these services
   * are tightly to Ehcache operations. You should be <b>really</b> cautious when using them to prevent disrupting Ehcache.
   *
   * @return the service provider used by this cache manager
   */
  ServiceProvider<Service> getServiceProvider();
}
