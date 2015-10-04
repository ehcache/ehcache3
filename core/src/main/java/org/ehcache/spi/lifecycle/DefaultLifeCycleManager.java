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

  private final CopyOnWriteArrayList<ListenerWrapper<?>> listeners = new CopyOnWriteArrayList<ListenerWrapper<?>>();
  private final ListenerWrapper<CacheManager> refCleaner = new ListenerWrapper<CacheManager>(CacheManager.class, new LifeCycleListenerAdapter<CacheManager>() {
    @Override
    public void afterClosing(CacheManager instance) {
      listeners.clear();
    }
  });

  @Override
  public <T> void register(Class<T> listenedType, LifeCycleListener<T> listener) {
    listeners.add(new ListenerWrapper<T>(listenedType, listener));
  }

  @Override
  public void unregister(LifeCycleListener<?> listener) {
    for (ListenerWrapper<?> l : listeners) {
      if (l.listener == listener) {
        listeners.remove(l);
      }
    }
  }

  @Override
  public void start(ServiceProvider serviceProvider) {
    // just to make sure all listeners are cleared when the cache manager is closed
    if (!listeners.contains(refCleaner)) {
      listeners.add(refCleaner);
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

  private static class ListenerWrapper<T> {
    final Class<T> type;
    final LifeCycleListener<T> listener;

    ListenerWrapper(Class<T> type, LifeCycleListener<T> listener) {
      this.type = type;
      this.listener = listener;
    }
  }

  @SuppressWarnings("unchecked")
  private <T> void initialized(T o) {
    for (ListenerWrapper<?> listenerWrapper : listeners) {
      if (listenerWrapper.type.isInstance(o)) {
        ((LifeCycleListener<T>) listenerWrapper.listener).afterInitialization(o);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private <T> void closed(T o) {
    ListIterator<ListenerWrapper<?>> it = listeners.listIterator(listeners.size());
    while (it.hasPrevious()) {
      ListenerWrapper<?> listenerWrapper = it.previous();
      if (listenerWrapper.type.isInstance(o)) {
        ((LifeCycleListener<T>) listenerWrapper.listener).afterClosing(o);
      }
    }
  }

}
