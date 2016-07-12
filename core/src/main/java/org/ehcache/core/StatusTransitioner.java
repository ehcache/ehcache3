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

package org.ehcache.core;

import org.ehcache.Status;
import org.ehcache.core.events.StateChangeListener;
import org.ehcache.StateTransitionException;
import org.ehcache.core.spi.LifeCycled;
import org.slf4j.Logger;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Alex Snaps
 */
final class StatusTransitioner {

  private final AtomicReference<InternalStatus.Transition> currentState;
  private volatile Thread maintenanceLease;
  private final Logger logger;

  private final CopyOnWriteArrayList<LifeCycled> hooks = new CopyOnWriteArrayList<LifeCycled>();
  private final CopyOnWriteArrayList<StateChangeListener> listeners = new CopyOnWriteArrayList<StateChangeListener>();

  StatusTransitioner(Logger logger) {
    this.currentState = new AtomicReference<InternalStatus.Transition>(InternalStatus.initial());
    this.logger = logger;
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
    logger.trace("Initializing");
    InternalStatus.Transition st;
    for (InternalStatus.Transition cs; !currentState.compareAndSet(cs = currentState.get(), st = cs.get().init()););
    return new Transition(st, null, "Initialize");
  }

  Transition close() {
    logger.trace("Closing");
    InternalStatus.Transition st;
    if(maintenanceLease != null && Thread.currentThread() != maintenanceLease) {
      throw new IllegalStateException("You don't own this MAINTENANCE lease");
    }
    for (InternalStatus.Transition cs; !currentState.compareAndSet(cs = currentState.get(), st = cs.get().close()););
    return new Transition(st, null, "Close");
  }

  Transition maintenance() {
    logger.trace("Entering Maintenance");
    InternalStatus.Transition st;
    for (InternalStatus.Transition cs; !currentState.compareAndSet(cs = currentState.get(), st = cs.get().maintenance()););
    return new Transition(st, Thread.currentThread(), "Enter Maintenance");
  }

  Transition exitMaintenance() {
    checkMaintenance();
    logger.trace("Exiting Maintenance");
    InternalStatus.Transition st;
    for (InternalStatus.Transition cs; !currentState.compareAndSet(cs = currentState.get(), st = cs.get().close()););
    return new Transition(st, Thread.currentThread(), "Exit Maintenance");
  }

  void addHook(LifeCycled hook) {
    validateHookRegistration();
    hooks.add(hook);
  }

  void removeHook(LifeCycled hook) {
    validateHookRegistration();
    hooks.remove(hook);
  }

  private void validateHookRegistration() {
    if(currentStatus() != Status.UNINITIALIZED) {
      throw new IllegalStateException("Can't modify hooks when not in " + Status.UNINITIALIZED);
    }
  }

  void registerListener(StateChangeListener listener) {
    if(!listeners.contains(listener)) {
      listeners.add(listener);
    }
  }

  void deregisterListener(StateChangeListener listener) {
    listeners.remove(listener);
  }

  private void runInitHooks() throws Exception {
    Deque<LifeCycled> initiated = new ArrayDeque<LifeCycled>();
    for (LifeCycled hook : hooks) {
      try {
        hook.init();
        initiated.add(hook);
      } catch (Exception initException) {
        while (!initiated.isEmpty()) {
          try {
            initiated.pop().close();
          } catch (Exception closeException) {
            logger.error("Failed to close() while shutting down because of .init() having thrown", closeException);
          }
        }
        throw initException;
      }
    }
  }

  private void runCloseHooks() throws Exception {
    Deque<LifeCycled> initiated = new ArrayDeque<LifeCycled>();
    for (LifeCycled hook : hooks) {
      initiated.addFirst(hook);
    }
    Exception firstFailure = null;
    while (!initiated.isEmpty()) {
      try {
        initiated.pop().close();
      } catch (Exception closeException) {
        if (firstFailure == null) {
          firstFailure = closeException;
        } else {
          logger.error("A LifeCyclable has thrown already while closing down", closeException);
        }
      }
    }
    if (firstFailure != null) {
      throw firstFailure;
    }
  }

  private void fireTransitionEvent(Status previousStatus, Status newStatus) {
    for (StateChangeListener listener : listeners) {
      listener.stateTransition(previousStatus, newStatus);
    }
  }

  final class Transition {

    private final InternalStatus.Transition st;
    private final Thread thread;
    private final String action;

    public Transition(final InternalStatus.Transition st, final Thread thread, final String action) {
      this.st = st;
      this.thread = thread;
      this.action = action;
    }

    public void succeeded() {
      try {
        switch(st.to()) {
          case AVAILABLE:
            runInitHooks();
            break;
          case UNINITIALIZED:
            maintenanceLease = null;
            runCloseHooks();
            break;
          case MAINTENANCE:
            maintenanceLease = thread;
            break;
          default:
            throw new IllegalArgumentException("Didn't expect that enum value: " + st.to());
        }
        st.succeeded();
      } catch (Exception e) {
        st.failed();
        throw new StateTransitionException(e);
      }

      try {
        fireTransitionEvent(st.from().toPublicStatus(), st.to().toPublicStatus());
      } finally {
        maintenanceLease = thread;
        logger.debug("{} successful.", action);
      }
    }

    public StateTransitionException failed(Throwable t) {
      if (st.done()) {
        if (t != null) {
          throw (AssertionError) new AssertionError("Throwable cannot be thrown if Transition is done.").initCause(t);
        }
        return null;
      }
      st.failed();
      if (t == null) {
        return null;
      }
      logger.error("{} failed.", action);
      if(t instanceof StateTransitionException) {
        return (StateTransitionException) t;
      }
      return new StateTransitionException(t);
    }
  }
}
