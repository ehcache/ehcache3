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

import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.terracotta.connection.Connection;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Properties;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class ConnectionStateTest {

  private static URI CLUSTER_URI = URI.create("terracotta://localhost:9510");

  private final ClusteringServiceConfiguration serviceConfiguration = ClusteringServiceConfigurationBuilder
          .cluster(CLUSTER_URI)
          .autoCreate(c -> c)
          .build();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void definePassthroughServer() {
    UnitTestConnectionService.add(CLUSTER_URI,
            new UnitTestConnectionService.PassthroughServerBuilder()
                    .resource("primary-server-resource", 64, MemoryUnit.MB)
                    .resource("secondary-server-resource", 64, MemoryUnit.MB)
                    .build());
  }

  @After
  public void removePassthrough() {
    expectedException.expect(IllegalStateException.class);
    UnitTestConnectionService.remove(CLUSTER_URI);
  }

  @Test
  public void testInitializeStateAfterConnectionCloses() throws Exception {

    ConnectionState connectionState = new ConnectionState(Timeouts.DEFAULT, new Properties(), serviceConfiguration);
    connectionState.initClusterConnection(Runnable::run);

    closeConnection();

    expectedException.expect(IllegalStateException.class);
    connectionState.getConnection().close();

    connectionState.initializeState();

    assertThat(connectionState.getConnection(), notNullValue());
    assertThat(connectionState.getEntityFactory(), notNullValue());

    connectionState.getConnection().close();

  }

  @Test
  public void testCreateClusterTierEntityAfterConnectionCloses() throws Exception {

    ConnectionState connectionState = new ConnectionState(Timeouts.DEFAULT, new Properties(), serviceConfiguration);
    connectionState.initClusterConnection(Runnable::run);
    connectionState.initializeState();

    closeConnection();

    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 4, MemoryUnit.MB);
    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(resourcePool.getPoolAllocation(),
            Long.class.getName(), String.class.getName(), LongSerializer.class.getName(), StringSerializer.class.getName(), null, false);

    ClusterTierClientEntity clientEntity = connectionState.createClusterTierClientEntity("cache1", serverStoreConfiguration, false);

    assertThat(clientEntity, notNullValue());

  }

  //For test to simulate connection close as result of lease expiry
  private void closeConnection() throws IOException {
    Collection<Connection> connections = UnitTestConnectionService.getConnections(CLUSTER_URI);

    assertThat(connections.size(), is(1));

    connections.iterator().next().close();
  }

}
