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

package org.ehcache.impl.store;

import org.ehcache.core.events.StoreEventSink;
import org.ehcache.impl.internal.events.AbstractStoreEventDispatcher;

/**
 * The default {@link org.ehcache.core.events.StoreEventDispatcher} implementation.
 */
public class DefaultStoreEventDispatcher<K, V> extends AbstractStoreEventDispatcher<K, V> {

  public DefaultStoreEventDispatcher(int dispatcherConcurrency) {
    super(dispatcherConcurrency);
  }

  @Override
  public StoreEventSink<K, V> eventSink() {
    if (getListeners().isEmpty()) {
      @SuppressWarnings("unchecked")
      StoreEventSink<K, V> noOpEventSink = (StoreEventSink<K, V>) NO_OP_EVENT_SINK;
      return noOpEventSink;
    } else {
      return super.eventSink();
    }
  }
}
