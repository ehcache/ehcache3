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

package org.ehcache.impl.internal.events;

import org.ehcache.core.events.StoreEventSink;

/**
 * ThreadLocalStoreEventDispatcher
 */
public class ThreadLocalStoreEventDispatcher<K, V> extends AbstractStoreEventDispatcher<K, V> {

  private final ThreadLocal<StoreEventSink<K, V>> tlEventSink = new ThreadLocal<StoreEventSink<K, V>>();
  private final ThreadLocal<Integer> usageDepth = new ThreadLocal<Integer>();

  public ThreadLocalStoreEventDispatcher(int dispatcherConcurrency) {
    super(dispatcherConcurrency);
  }

  @Override
  public StoreEventSink<K, V> eventSink() {
    if (getListeners().isEmpty()) {
      return NO_OP_EVENT_SINK;
    } else {
      StoreEventSink<K, V> eventSink = tlEventSink.get();
      if (eventSink == null) {
        eventSink = new FudgingInvocationScopedEventSink<K, V>(getFilters(), isEventOrdering(), getOrderedQueues(), getListeners());
        tlEventSink.set(eventSink);
        usageDepth.set(0);
      } else {
        usageDepth.set(usageDepth.get() + 1);
      }
      return eventSink;
    }
  }

  @Override
  public void releaseEventSink(StoreEventSink<K, V> eventSink) {
    if (eventSink != NO_OP_EVENT_SINK) {
      int depthValue = usageDepth.get();
      if (depthValue == 0) {
        try {
          super.releaseEventSink(eventSink);
        } finally {
          tlEventSink.remove();
          usageDepth.remove();
        }
      } else {
        usageDepth.set(depthValue - 1);
      }
    }
  }

  @Override
  public void releaseEventSinkAfterFailure(StoreEventSink<K, V> eventSink, Throwable throwable) {
    if (eventSink != NO_OP_EVENT_SINK) {
      int depthValue = usageDepth.get();
      if (depthValue == 0) {
        try {
          super.releaseEventSinkAfterFailure(eventSink, throwable);
        } finally {
          tlEventSink.remove();
          usageDepth.remove();
        }
      } else {
        usageDepth.set(depthValue - 1);
      }
    }
  }
}
