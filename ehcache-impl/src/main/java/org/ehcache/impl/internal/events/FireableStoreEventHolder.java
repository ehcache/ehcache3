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

import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventListener;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * FireableStoreEventHolder
 */
class FireableStoreEventHolder<K, V> {

  enum Status {
    CREATED, FIREABLE, FIRED
  }

  private final Lock lock = new ReentrantLock();
  private final AtomicReference<Status> status = new AtomicReference<>(Status.CREATED);
  private volatile boolean failed = false;

  private final StoreEvent<K, V> event;
  private final Condition condition;

  FireableStoreEventHolder(StoreEvent<K, V> event) {
    this.event = event;
    this.condition = lock.newCondition();
  }

  void markFireable() {
    status.compareAndSet(Status.CREATED, Status.FIREABLE);
  }

  boolean isFireable() {
    return status.get().equals(Status.FIREABLE);
  }

  void waitTillFired() {
    while (!isFired()) {
      lock.lock();
      try {
        if (!isFired()) {
          condition.awaitUninterruptibly();
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private boolean isFired() {
    return status.get() == Status.FIRED;
  }

  boolean markFired() {
    boolean didIt = status.compareAndSet(Status.FIREABLE, Status.FIRED);
    if (didIt) {
      lock.lock();
      try {
        condition.signal();
      } finally {
        lock.unlock();
      }
    }
    return didIt;
  }

  void markFailed() {
    failed = true;
  }

  void fireOn(StoreEventListener<K, V> listener) {
    if (!failed) {
      listener.onEvent(event);
    }
  }

  int eventKeyHash() {
    return event.getKey().hashCode();
  }

  StoreEvent<K, V> getEvent() {
    return event;
  }

  @Override
  public String toString() {
    return "FireableStoreEventHolder in state " + status.get() + " of " + event + (failed ? " (failed)":" (not failed)");
  }
}
