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

package org.ehcache;

import org.ehcache.events.StateChangeListener;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Alex Snaps
 */
final class StatusTransitioner {

  private final AtomicReference<InternalStatus.Transition> currentState;
  private volatile Thread maintenanceLease;

  private final CopyOnWriteArrayList<StateChangeListener> listeners = new CopyOnWriteArrayList<StateChangeListener>();

  StatusTransitioner() {
    this.currentState = new AtomicReference<InternalStatus.Transition>(InternalStatus.initial());
  }

  Status currentStatus() {
    return currentState.get().get().toPublicStatus();
  }

  boolean isTransitioning() {
    return !currentState.get().done();
  }

  void checkAvailable() {
    final Status status = currentStatus();
    if(status == Status.MAINTENANCE && Thread.currentThread() != maintenanceLease) {
      throw new IllegalStateException("State is " + status + ", yet you don't own it!");
    } else if(status == Status.UNINITIALIZED) {
      throw new IllegalStateException("State is " + status);
    }
  }

  void checkMaintenance() {
    final Status status = currentStatus();
    if(status == Status.MAINTENANCE && Thread.currentThread() != maintenanceLease) {
      throw new IllegalStateException("State is " + status + ", yet you don't own it!");
    } else if (status != Status.MAINTENANCE) {
      throw new IllegalStateException("State is " + status);
    }
  }

  Transition init() {
    InternalStatus.Transition st;
    for (InternalStatus.Transition cs; !currentState.compareAndSet(cs = currentState.get(), st = cs.get().init()););
    return new Transition(st, null);
  }

  Transition close() {
    InternalStatus.Transition st;
    if(maintenanceLease != null && Thread.currentThread() != maintenanceLease) {
      throw new IllegalStateException("You don't own this MAINTENANCE lease");
    }
    for (InternalStatus.Transition cs; !currentState.compareAndSet(cs = currentState.get(), st = cs.get().close()););
    return new Transition(st, null);
  }

  Transition maintenance() {
    InternalStatus.Transition st;
    for (InternalStatus.Transition cs; !currentState.compareAndSet(cs = currentState.get(), st = cs.get().maintenance()););
    return new Transition(st, Thread.currentThread());
  }

  void registerListener(StateChangeListener listener) {
    if(!listeners.contains(listener)) {
      listeners.add(listener);
    }
  }

  void deregisterListener(StateChangeListener listener) {
    listeners.remove(listener);
  }

  private void fireTransitionEvent(Status previousStatus, Status newStatus) {
    for (StateChangeListener listener : listeners) {
      listener.stateTransition(previousStatus, newStatus);
    }
  }

  final class Transition {

    private final InternalStatus.Transition st;
    private final Thread thread;

    public Transition(final InternalStatus.Transition st, final Thread thread) {
      this.st = st;
      this.thread = thread;
    }

    public void succeeded() {
      try {
        fireTransitionEvent(st.from().toPublicStatus(), st.to().toPublicStatus());
      } finally {
        maintenanceLease = thread;
        st.succeeded();
      }
    }

    public void failed() {
      st.failed();
    }
  }
}
