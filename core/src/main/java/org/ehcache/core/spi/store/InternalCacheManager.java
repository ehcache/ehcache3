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

/**
 * The {@code Service}-facing version of a {@code CacheManager}.  This interface adds
 * methods used internally by service implementations.
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
}
