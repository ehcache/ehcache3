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
package org.ehcache.clustered.client.internal.service;

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.MockConnectionService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.terracotta.connection.Connection;
import org.terracotta.exception.ConnectionShutdownException;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ReconnectTest {

  private static URI CLUSTER_URI = URI.create("mock://localhost:9510");

  private final ClusteringServiceConfiguration serviceConfiguration = ClusteringServiceConfigurationBuilder
          .cluster(CLUSTER_URI)
          .autoCreate(c -> c)
          .build();

  @Test(expected = RuntimeException.class)
  public void testInitialConnectDoesNotRetryAfterConnectionException() {
    MockConnectionService.mockConnection = null;
    ConnectionState connectionState = new ConnectionState(Timeouts.DEFAULT, new Properties(), serviceConfiguration);

    connectionState.initClusterConnection(Runnable::run);
  }

  @Test
  public void testAfterConnectionReconnectHappensEvenAfterConnectionException() throws Exception {
    Connection connection = Mockito.mock(Connection.class, Mockito.withSettings()
            .defaultAnswer(invocation -> {
              throw new ConnectionShutdownException("Connection Closed");
            }));

    MockConnectionService.mockConnection = connection;

    ConnectionState connectionState = new ConnectionState(Timeouts.DEFAULT, new Properties(), serviceConfiguration);

    connectionState.initClusterConnection(Runnable::run);

    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> connectionState.initializeState());

    MockConnectionService.mockConnection = null;

    CompletableFuture<Void> reconnecting = CompletableFuture.runAsync(() -> {
      MockConnectionService.mockConnection = Mockito.mock(Connection.class, Mockito.withSettings().defaultAnswer(invocation -> {
        throw new RuntimeException("Stop reconnecting");
      }));
      while (connectionState.getReconnectCount() == 1) {
        break;
      }
    });

    reconnecting.get();

    try {
      future.get();
    } catch (ExecutionException e) {
      Assert.assertThat(e.getCause().getMessage(), Matchers.is("Stop reconnecting"));
    }

    Assert.assertThat(connectionState.getReconnectCount(), Matchers.is(1));

  }

}
