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

import org.ehcache.CacheManager;
import org.ehcache.Status;
import org.ehcache.events.StateChangeListener;
import org.ehcache.spi.ServiceProvider;

import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Mathieu Carbou
 */
public class DefaultLifeCycleManager implements LifeCycleManager, LifeCycleService {

  private final CopyOnWriteArrayList<Listener<?>> listeners = new CopyOnWriteArrayList<Listener<?>>();
  private final LifeCycleListenerAdapter<CacheManager> refCleaner = new LifeCycleListenerAdapter<CacheManager>() {
    @Override
    public void afterClosing(CacheManager instance) {
      listeners.clear();
    }
  };

  @Override
  public <T> void register(Class<T> listenedType, LifeCycleListener<T> listener) {
    listeners.add(new Listener<T>(listenedType, listener));
  }

  @Override
  public void unregister(LifeCycleListener<?> listener) {
    for (Listener<?> l : listeners) {
      if (l.listener == listener) {
        listeners.remove(l);
      }
    }
  }

  @Override
  public void start(ServiceProvider serviceProvider) {
    // just to make sure all listeners are cleared when the cache manager is closed
    if (!listeners.contains(refCleaner)) {
      register(CacheManager.class, refCleaner);
    }
  }

  @Override
  public void stop() {
  }

  @Override
  public StateChangeListener createStateChangeListener(final Object o) {
    return new StateChangeListener() {
      @Override
      public void stateTransition(Status from, Status to) {
        if (from == Status.UNINITIALIZED && to == Status.AVAILABLE) {
          initialized(o);
        } else if (from == Status.AVAILABLE && to == Status.UNINITIALIZED) {
          closed(o);
        }
      }
    };
  }

  private static class Listener<T> {
    final Class<T> type;
    final LifeCycleListener<T> listener;

    Listener(Class<T> type, LifeCycleListener<T> listener) {
      this.type = type;
      this.listener = listener;
    }
  }

  @SuppressWarnings("unchecked")
  private <T> void initialized(T o) {
    for (Listener<?> listener : listeners) {
      if (listener.type.isInstance(o)) {
        ((LifeCycleListener<T>) listener.listener).afterInitialization(o);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private <T> void closed(T o) {
    ListIterator<Listener<?>> it = listeners.listIterator(listeners.size());
    while (it.hasPrevious()) {
      Listener<?> listener = it.previous();
      if (listener.type.isInstance(o)) {
        ((LifeCycleListener<T>) listener.listener).afterClosing(o);
      }
    }
  }

}
