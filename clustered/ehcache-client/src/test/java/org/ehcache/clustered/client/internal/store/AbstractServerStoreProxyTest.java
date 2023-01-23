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
package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityFactory;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService.PassthroughServerBuilder;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.ClusterTierManagerServerEntityService;
import org.ehcache.clustered.server.store.ObservableClusterTierServerEntityService;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.serialization.LongSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.terracotta.connection.Connection;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public abstract class AbstractServerStoreProxyTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://localhost");
  private static final UnitTestConnectionService CONNECTION_SERVICE = new UnitTestConnectionService();

  protected static ObservableClusterTierServerEntityService observableClusterTierService;

  @BeforeClass
  public static void createCluster() {
    UnitTestConnectionService.add(CLUSTER_URI, new PassthroughServerBuilder()
      .serverEntityService(new ClusterTierManagerServerEntityService())
      .clientEntityService(new ClusterTierManagerClientEntityService())
      .serverEntityService(observableClusterTierService = new ObservableClusterTierServerEntityService())
      .clientEntityService(new ClusterTierClientEntityService())
      .serverEntityService(new VoltronReadWriteLockServerEntityService())
      .clientEntityService(new VoltronReadWriteLockEntityClientService())
      .resource("defaultResource", 128, MemoryUnit.MB).build());
  }

  @AfterClass
  public static void destroyCluster() {
    UnitTestConnectionService.remove(CLUSTER_URI);
    observableClusterTierService = null;
  }

  protected static SimpleClusterTierClientEntity createClientEntity(String name,
                                                                  ServerStoreConfiguration configuration,
                                                                  boolean create) throws Exception {
    return createClientEntity(name, configuration, create, true);
  }

  protected static SimpleClusterTierClientEntity createClientEntity(String name,
                                                                    ServerStoreConfiguration configuration,
                                                                    boolean create,
                                                                    boolean validate) throws Exception {
    Connection connection = CONNECTION_SERVICE.connect(CLUSTER_URI, new Properties());

    // Create ClusterTierManagerClientEntity if needed
    ClusterTierManagerClientEntityFactory entityFactory = new ClusterTierManagerClientEntityFactory(
      connection, Runnable::run,
      TimeoutsBuilder.timeouts().write(Duration.ofSeconds(30)).build());
    if (create) {
      entityFactory.create(name, new ServerSideConfiguration("defaultResource", Collections.emptyMap()));
    }
    // Create or fetch the ClusterTierClientEntity
    SimpleClusterTierClientEntity clientEntity = (SimpleClusterTierClientEntity) entityFactory.fetchOrCreateClusteredStoreEntity(name, name, configuration, create ? ClusteringServiceConfiguration.ClientMode.AUTO_CREATE : ClusteringServiceConfiguration.ClientMode.CONNECT, false);
    if (validate) {
      clientEntity.validate(configuration);
    }
    return clientEntity;
  }

  protected static SimpleClusterTierClientEntity createClientEntity(String name, Consistency consistency, boolean create) throws Exception {
    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(8L, MemoryUnit.MB);

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(resourcePool.getPoolAllocation(), Long.class
      .getName(),
      Long.class.getName(), LongSerializer.class.getName(), LongSerializer.class
      .getName(), consistency, false);

    return createClientEntity(name, serverStoreConfiguration, create);
  }

}
