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
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.spi.LifeCycled;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

public class StatusTransitionerTest {

  @Test
  public void testTransitionsToLowestStateOnFailure() {
    StatusTransitioner transitioner = new StatusTransitioner(LoggerFactory.getLogger(StatusTransitionerTest.class));
    assertThat(transitioner.currentStatus(), is(Status.UNINITIALIZED));
    transitioner.init().failed(null);
    assertThat(transitioner.currentStatus(), is(Status.UNINITIALIZED));
    transitioner.init().succeeded();
    assertThat(transitioner.currentStatus(), is(Status.AVAILABLE));
    transitioner.close().failed(null);
    assertThat(transitioner.currentStatus(), is(Status.UNINITIALIZED));
  }

  @Test
  public void testFiresListeners() {
    StatusTransitioner transitioner = new StatusTransitioner(LoggerFactory.getLogger(StatusTransitionerTest.class));
    final StateChangeListener listener = mock(StateChangeListener.class);
    transitioner.registerListener(listener);
    transitioner.init().succeeded();
    verify(listener).stateTransition(Status.UNINITIALIZED, Status.AVAILABLE);
    reset(listener);
    transitioner.deregisterListener(listener);
    transitioner.close().succeeded();
    verify(listener, never()).stateTransition(Status.AVAILABLE, Status.UNINITIALIZED);
  }

  @Test
  public void testFinishesTransitionOnListenerThrowing() {
    StatusTransitioner transitioner = new StatusTransitioner(LoggerFactory.getLogger(StatusTransitionerTest.class));
    final StateChangeListener listener = mock(StateChangeListener.class);
    final RuntimeException runtimeException = new RuntimeException();
    doThrow(runtimeException).when(listener).stateTransition(Status.UNINITIALIZED, Status.AVAILABLE);
    transitioner.registerListener(listener);
    try {
      transitioner.init().succeeded();
      fail();
    } catch (RuntimeException e) {
      assertThat(e, is(runtimeException));
    }
    assertThat(transitioner.currentStatus(), is(Status.AVAILABLE));
  }

  @Test
  public void testMaintenanceOnlyLetsTheOwningThreadInteract() throws InterruptedException {
    final StatusTransitioner transitioner = new StatusTransitioner(LoggerFactory.getLogger(StatusTransitionerTest.class));
    transitioner.maintenance().succeeded();
    transitioner.checkMaintenance();
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          transitioner.checkMaintenance();
          fail();
        } catch (IllegalStateException e) {
          assertThat(e.getMessage().contains(Status.MAINTENANCE.name()), is(true));
          assertThat(e.getMessage().contains("own"), is(true));
        }
      }
    };
    thread.start();
    thread.join();
  }

  @Test
  public void testMaintenanceOnlyOwningThreadCanClose() throws InterruptedException {
    final StatusTransitioner transitioner = new StatusTransitioner(LoggerFactory.getLogger(StatusTransitionerTest.class));
    transitioner.maintenance().succeeded();
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          transitioner.close();
          fail();
        } catch (IllegalStateException e) {
          assertThat(e.getMessage().contains(Status.MAINTENANCE.name()), is(true));
          assertThat(e.getMessage().contains("own"), is(true));
        }
      }
    };
    thread.start();
    thread.join();
    transitioner.close();
  }

  @Test
  public void testCheckAvailableAccountsForThreadLease() throws InterruptedException {
    final StatusTransitioner transitioner = new StatusTransitioner(LoggerFactory.getLogger(StatusTransitionerTest.class));
    transitioner.maintenance().succeeded();
    transitioner.checkAvailable();
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          transitioner.checkAvailable();
          fail();
        } catch (IllegalStateException e) {
          assertThat(e.getMessage().contains(Status.MAINTENANCE.name()), is(true));
          assertThat(e.getMessage().contains("own"), is(true));
        }
      }
    };
    thread.start();
    thread.join();
    transitioner.close();
  }

  @Test
  public void testHookThrowingVetosTransition() throws Exception {
    final StatusTransitioner transitioner = new StatusTransitioner(LoggerFactory.getLogger(StatusTransitionerTest.class));
    final LifeCycled mock = mock(LifeCycled.class);
    transitioner.addHook(mock);
    final Exception toBeThrown = new Exception();
    doThrow(toBeThrown).when(mock).init();
    try {
      transitioner.init().succeeded();
      fail();
    } catch (StateTransitionException e) {
      assertThat(e.getCause(), CoreMatchers.<Throwable>sameInstance(toBeThrown));
    }
    assertThat(transitioner.currentStatus(), is(Status.UNINITIALIZED));
    reset(mock);
    doThrow(toBeThrown).when(mock).close();
    transitioner.init().succeeded();
    try {
      transitioner.close().succeeded();
      fail();
    } catch (StateTransitionException e) {
      assertThat(e.getCause(), CoreMatchers.<Throwable>sameInstance(toBeThrown));
    }
  }

  @Test
  public void testRepectRegistrationOrder() {

    final List<LifeCycled> order = new ArrayList<LifeCycled>();

    final StatusTransitioner transitioner = new StatusTransitioner(LoggerFactory.getLogger(StatusTransitionerTest.class));

    final Recorder first = new Recorder(order, "first");
    final Recorder second = new Recorder(order, "second");

    transitioner.addHook(first);
    transitioner.addHook(second);
    transitioner.init().succeeded();
    assertThat(order.get(0), CoreMatchers.<LifeCycled>sameInstance(first));
    assertThat(order.get(1), CoreMatchers.<LifeCycled>sameInstance(second));
    order.clear();
    transitioner.close().succeeded();
    assertThat(order.get(0), CoreMatchers.<LifeCycled>sameInstance(second));
    assertThat(order.get(1), CoreMatchers.<LifeCycled>sameInstance(first));
  }

  @Test
  public void testStopsInitedHooksOnFailure() throws Exception {
    final StatusTransitioner transitioner = new StatusTransitioner(LoggerFactory.getLogger(StatusTransitionerTest.class));
    final LifeCycled first = mock(LifeCycled.class);
    final LifeCycled second = mock(LifeCycled.class);
    transitioner.addHook(first);
    transitioner.addHook(second);
    final Exception toBeThrown = new Exception();
    doThrow(toBeThrown).when(second).init();
    try {
      transitioner.init().succeeded();
      fail();
    } catch (StateTransitionException e) {
      // expected
    }
    verify(first).init();
    verify(first).close();
  }

  @Test
  public void testDoesNoReInitsClosedHooksOnFailure() throws Exception {
    final StatusTransitioner transitioner = new StatusTransitioner(LoggerFactory.getLogger(StatusTransitionerTest.class));
    final LifeCycled first = mock(LifeCycled.class);
    final LifeCycled second = mock(LifeCycled.class);
    transitioner.addHook(first);
    transitioner.addHook(second);
    transitioner.init().succeeded();
    reset(first);
    reset(second);
    final Exception toBeThrown = new Exception();
    doThrow(toBeThrown).when(first).close();
    try {
      transitioner.close().succeeded();
      fail();
    } catch (StateTransitionException e) {
      // expected
    }
    verify(second).close();
    verify(second, never()).init();
  }

  @Test
  public void testClosesAllHooksOnFailure() throws Exception {
    final StatusTransitioner transitioner = new StatusTransitioner(LoggerFactory.getLogger(StatusTransitionerTest.class));
    final LifeCycled first = mock(LifeCycled.class);
    final LifeCycled second = mock(LifeCycled.class);
    transitioner.addHook(first);
    transitioner.addHook(second);
    transitioner.init().succeeded();
    reset(first);
    reset(second);
    final Exception toBeThrown = new Exception();
    doThrow(toBeThrown).when(second).close();
    try {
      transitioner.close().succeeded();
      fail();
    } catch (StateTransitionException e) {
      // expected
    }
    verify(first).close();
  }

  private static class Recorder implements LifeCycled {
    private final List<LifeCycled> order;
    private final String name;

    public Recorder(final List<LifeCycled> order, final String name) {
      this.order = order;
      this.name = name;
    }

    @Override
    public void init() throws Exception {
      order.add(this);
    }

    @Override
    public void close() throws Exception {
      order.add(this);
    }

    @Override
    public String toString() {
      return "Recorder{" + name + '}';
    }
  }
}