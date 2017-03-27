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

package org.ehcache.clustered.server.state;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ClientMessageTrackerTest {

  @Test
  public void testReconcilationOfClients() throws Exception {

    ClientMessageTracker clientMessageTracker = new DefaultClientMessageTracker();
    UUID clientId = UUID.randomUUID();
    clientMessageTracker.applied(20L, clientId);

    clientMessageTracker.reconcileTrackedClients(Collections.singleton(clientId));

    Map messageTracker = getMessageTracker(clientMessageTracker);
    assertThat(messageTracker.size(), is(1));

    clientMessageTracker.reconcileTrackedClients(Collections.singleton(UUID.randomUUID()));

    assertThat(messageTracker.size(), is(0));

  }

  @Test
  public void testClientsAreTrackedLazily() throws Exception {

    ClientMessageTracker clientMessageTracker = new DefaultClientMessageTracker();
    Map messageTracker = getMessageTracker(clientMessageTracker);
    assertThat(messageTracker.size(), is(0));
    clientMessageTracker.applied(20L, UUID.randomUUID());
    assertThat(messageTracker.size(), is(1));

  }

  @Test
  public void testStopTracking() throws Exception {

    ClientMessageTracker clientMessageTracker = new DefaultClientMessageTracker();
    clientMessageTracker.applied(20L, UUID.randomUUID());
    clientMessageTracker.applied(21L, UUID.randomUUID());
    Map messageTracker = getMessageTracker(clientMessageTracker);
    assertThat(messageTracker.size(), is(2));

    clientMessageTracker.stopTracking();

    clientMessageTracker.applied(22L, UUID.randomUUID());
    clientMessageTracker.applied(23L, UUID.randomUUID());
    assertThat(messageTracker.size(), is(2)); // The same old 2. No increments by the last 2 applied calls

  }

  @Test
  public void testMessageTrackerConcurrency() throws Exception {
    final int highestContiguousId = 1000;
    final int numTasks = 20;
    final int maxApplications = 2;

    ClientMessageTracker clientMessageTracker = new DefaultClientMessageTracker();
    ExecutorService executor = Executors.newFixedThreadPool(20);
    for (int p = 0; p < 2; p++) {
      List<Future<?>> tasks = new ArrayList<>();
      final UUID clientId = UUID.randomUUID();
      clientMessageTracker.applied(0, clientId);
      for (int i = 0; i < numTasks; i++) {
        final int j = i;
        tasks.add(executor.submit(() -> {
          for (long k = j; k < highestContiguousId; k += numTasks) {
            final UUID otherClientId = UUID.randomUUID();
            for (int l = 0; l < maxApplications; l++) {
              clientMessageTracker.applied((highestContiguousId * l) + k, clientId);
            }
            clientMessageTracker.applied(k, otherClientId);
            Thread.yield();
            for (int l = 0; l < maxApplications; l++) {
              assertThat(clientMessageTracker.isDuplicate((highestContiguousId * l) + k, clientId), is(true));
            }
            clientMessageTracker.remove(otherClientId);
          }
        }));
      }

      tasks.forEach((future) -> {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
          fail("Unexpected Exception " + e.getMessage());
        }
      });
    }
  }

  private Map getMessageTracker(ClientMessageTracker clientMessageTracker) throws Exception {
    Field field = clientMessageTracker.getClass().getDeclaredField("clientUUIDMessageTrackerMap");
    field.setAccessible(true);
    return (Map)field.get(clientMessageTracker);
  }
}
