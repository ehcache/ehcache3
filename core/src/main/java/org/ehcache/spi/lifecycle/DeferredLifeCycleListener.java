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

import org.ehcache.util.Deferred;

/**
 * @author Mathieu Carbou
 */
public final class DeferredLifeCycleListener<T> implements LifeCycleListener<T> {
  
  private final Deferred<T> afterInitialization = Deferred.create();
  private final Deferred<T> afterClosing = Deferred.create();
  
  @Override
  public void afterInitialization(T instance) {
    afterInitialization.resolve(instance);
  }

  @Override
  public void afterClosing(T instance) {
    afterClosing.resolve(instance);
  }

  public Deferred<T> afterInitialization() {
    return afterInitialization;
  }

  public Deferred<T> afterClosing() {
    return afterClosing;
  }
}
