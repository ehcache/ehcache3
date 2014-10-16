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
import org.junit.Test;

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
    StatusTransitioner transitioner = new StatusTransitioner();
    assertThat(transitioner.currentStatus(), is(Status.UNINITIALIZED));
    transitioner.init().failed();
    assertThat(transitioner.currentStatus(), is(Status.UNINITIALIZED));
    transitioner.init().succeeded();
    assertThat(transitioner.currentStatus(), is(Status.AVAILABLE));
    transitioner.close().failed();
    assertThat(transitioner.currentStatus(), is(Status.UNINITIALIZED));
  }

  @Test
  public void testFiresListeners() {
    StatusTransitioner transitioner = new StatusTransitioner();
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
    StatusTransitioner transitioner = new StatusTransitioner();
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
    final StatusTransitioner transitioner = new StatusTransitioner();
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
    final StatusTransitioner transitioner = new StatusTransitioner();
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

}