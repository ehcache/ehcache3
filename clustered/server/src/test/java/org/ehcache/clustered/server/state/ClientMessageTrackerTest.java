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
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ClientMessageTrackerTest {

  @Test
  public void testReconcilationOfClients() throws Exception {

    ClientMessageTracker clientMessageTracker = new ClientMessageTracker();
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

    ClientMessageTracker clientMessageTracker = new ClientMessageTracker();
    Map messageTracker = getMessageTracker(clientMessageTracker);
    assertThat(messageTracker.size(), is(0));
    clientMessageTracker.applied(20L, UUID.randomUUID());
    assertThat(messageTracker.size(), is(1));

  }

  private Map getMessageTracker(ClientMessageTracker clientMessageTracker) throws Exception {
    Field field = clientMessageTracker.getClass().getDeclaredField("messageTrackers");
    field.setAccessible(true);
    return (Map)field.get(clientMessageTracker);
  }
}
