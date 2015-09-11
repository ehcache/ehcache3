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

import org.ehcache.events.StateChangeListener;

/**
 * Service used internally to create a StateChangeListener based on an instance having a lifecycle.
 * <p/>
 * A StateChangeListener runs after all init / close hooks.
 *
 * @author Mathieu Carbou
 */
public interface LifeCycleManager {

  /**
   * Create a {@link StateChangeListener} instance based an object which will trigger all listeners of type {@link LifeCycleListener}
   * having registered for this object type with the {@link LifeCycleService}
   *
   * @param o The Object to be lifecycled
   * @return A listener that can be registered into a {@link org.ehcache.StatusTransitioner}
   */
  StateChangeListener createStateChangeListener(Object o);

}
