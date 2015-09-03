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

package org.ehcache.spi.lifecycle;

import org.ehcache.spi.service.Service;

/**
 * Service which can be used to register some listeners that will be triggers on initialization and close on some objects.
 *
 * @author Mathieu Carbou
 */
public interface LifeCycleService extends Service {

  /**
   * Registers a listener for the lifecycle of objects of a specific type
   *
   * @param listenedType The class of objects to listen to
   * @param listener     The listener
   * @param <T>          The type of object to listen
   */
  <T> void register(Class<T> listenedType, LifeCycleListener<T> listener);

  /**
   * Unregisters a listener instance
   * 
   * @param listener The listener to unregister
   */
  void unregister(LifeCycleListener<?> listener);
}
