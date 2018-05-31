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

package org.ehcache.xml.model;

import java.util.HashSet;
import java.util.Set;

public class ListenersConfig {

  private final String threadPool;
  private final Iterable<ListenersType.Listener> listeners;

  public ListenersConfig(final ListenersType type, final ListenersType... others) {
    String threadPool = type.getDispatcherThreadPool();
    Set<ListenersType.Listener> listenerSet = new HashSet<>();
    listenerSet.addAll(type.getListener());

    for (ListenersType other : others) {
      if (threadPool == null && other.getDispatcherThreadPool() != null) {
        threadPool = other.getDispatcherThreadPool();
      }
      listenerSet.addAll(other.getListener());
    }

    this.threadPool = threadPool;
    this.listeners = !listenerSet.isEmpty() ? listenerSet : null;
  }

  public String threadPool() {
    return threadPool;
  }

  public Iterable<ListenersType.Listener> listeners() {
    return listeners;
  }

}
