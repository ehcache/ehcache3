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

import org.ehcache.CachePersistenceException;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.EhcacheClientEntityService;
import org.ehcache.clustered.client.internal.EhcacheEntityCreationException;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService.PassthroughServerBuilder;
import org.ehcache.clustered.client.internal.config.FixedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.store.EventualServerStoreProxy;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.StrongServerStoreProxy;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.client.service.ClusteringService.ClusteredCacheIdentifier;
import org.ehcache.clustered.common.ClusteredStoreValidationException;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.server.ObservableEhcacheServerEntityService;
import org.ehcache.clustered.server.ObservableEhcacheServerEntityService.ObservableEhcacheActiveEntity;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.store.StoreEventSourceConfiguration;
import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.core.internal.store.StoreConfigurationImpl;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.consensus.entity.CoordinationServerEntityService;
import org.terracotta.consensus.entity.client.ClientCoordinationEntityService;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.exception.EntityNotFoundException;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.ehcache.clustered.client.config.ClusteredResourceType.Types.FIXED;
import static org.ehcache.clustered.client.config.ClusteredResourceType.Types.SHARED;
import static org.ehcache.clustered.client.internal.service.TestServiceProvider.providerContaining;
import org.ehcache.clustered.common.ClusteredStoreCreationException;
import static org.ehcache.config.ResourceType.Core.DISK;
import static org.ehcache.config.ResourceType.Core.HEAP;
import static org.ehcache.config.ResourceType.Core.OFFHEAP;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultClusteringServiceTest {

  private static final String CLUSTER_URI_BASE = "http://example.com:9540/";
  private ObservableEhcacheServerEntityService observableEhcacheServerEntityService;

  @Before
  public void definePassthroughServer() throws Exception {
    observableEhcacheServerEntityService = new ObservableEhcacheServerEntityService();
    UnitTestConnectionService.add(CLUSTER_URI_BASE,
        new PassthroughServerBuilder()
            .serverEntityService(observableEhcacheServerEntityService)
            .clientEntityService(new EhcacheClientEntityService())
            .serverEntityService(new CoordinationServerEntityService())
            .clientEntityService(new ClientCoordinationEntityService())
            .resource("defaultResource", 128, MemoryUnit.MB)
            .resource("serverResource1", 32, MemoryUnit.MB)
            .resource("serverResource2", 32, MemoryUnit.MB)
            .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    UnitTestConnectionService.remove(CLUSTER_URI_BASE);
    observableEhcacheServerEntityService = null;
  }

  @Test
  public void testHandlesResourceType() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);

    assertThat(service.handlesResourceType(DISK), is(false));
    assertThat(service.handlesResourceType(HEAP), is(false));
    assertThat(service.handlesResourceType(OFFHEAP), is(false));
    assertThat(service.handlesResourceType(FIXED), is(true));
    assertThat(service.handlesResourceType(SHARED), is(true));
    assertThat(service.handlesResourceType(new ClusteredResourceType<ClusteredResourcePool>() {
      @Override
      public Class<ClusteredResourcePool> getResourcePoolClass() {
        throw new UnsupportedOperationException(".getResourcePoolClass not implemented");
      }

      @Override
      public boolean isPersistable() {
        throw new UnsupportedOperationException(".isPersistable not implemented");
      }

      @Override
      public boolean requiresSerialization() {
        throw new UnsupportedOperationException(".requiresSerialization not implemented");
      }

      @Override
      public int getTierHeight() {
        throw new UnsupportedOperationException(".getTierHeight not implemented");
      }
    }), is(false));
  }

  @Test
  public void testGetPersistenceSpaceIdentifier() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);

    PersistableResourceService.PersistenceSpaceIdentifier spaceIdentifier = service.getPersistenceSpaceIdentifier("cacheAlias", null);
    assertThat(spaceIdentifier, is(instanceOf(ClusteredCacheIdentifier.class)));
    assertThat(((ClusteredCacheIdentifier)spaceIdentifier).getId(), is("cacheAlias"));
  }

  @Test
  public void testCreate() throws Exception {
    CacheConfigurationBuilder<Long, String> configBuilder =
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .with(ClusteredResourcePoolBuilder.shared("primary")));
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);

    PersistableResourceService.PersistenceSpaceIdentifier spaceIdentifier = service.getPersistenceSpaceIdentifier("cacheAlias", configBuilder
        .build());
    assertThat(spaceIdentifier, instanceOf(ClusteredCacheIdentifier.class));
    assertThat(((ClusteredCacheIdentifier) spaceIdentifier).getId(), is("cacheAlias"));
  }

  @Test
  public void testConnectionName() throws Exception {
    String entityIdentifier = "my-application";
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(
            URI.create(CLUSTER_URI_BASE + entityIdentifier),
            true,
            null,
            Collections.<String, ClusteringServiceConfiguration.PoolDefinition>emptyMap());
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    Collection<Properties> propsCollection = UnitTestConnectionService.getConnectionProperties(URI.create(CLUSTER_URI_BASE));
    assertThat(propsCollection.size(), is(1));
    Properties props = propsCollection.iterator().next();
    assertEquals(
        props.getProperty(ConnectionPropertyNames.CONNECTION_NAME),
        DefaultClusteringService.CONNECTION_PREFIX + entityIdentifier
    );
  }

  @Test
  public void testStartStopAutoCreate() throws Exception {
    URI clusterUri = URI.create(CLUSTER_URI_BASE + "my-application");
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(clusterUri)
            .autoCreate(true)
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    assertThat(UnitTestConnectionService.getConnectionProperties(clusterUri).size(), is(1));
    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));

    service.stop();

    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(UnitTestConnectionService.getConnectionProperties(clusterUri).size(), is(0));
  }

  @Test
  public void testStartStopNoAutoCreate() throws Exception {
    URI clusterUri = URI.create(CLUSTER_URI_BASE + "my-application");
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(clusterUri)
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    try {
      service.start(null);
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getCause(), is(instanceOf(EntityNotFoundException.class)));
    }

    assertThat(UnitTestConnectionService.getConnectionProperties(clusterUri).size(), is(0));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(0));

    service.stop();
  }

  /**
   * Ensures a second client specifying auto-create can start {@link DefaultClusteringService} while the
   * creator is still connected.
   */
  @Test
  public void testStartStopAutoCreateTwiceA() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .build();
    DefaultClusteringService firstService = new DefaultClusteringService(configuration);
    firstService.start(null);

    DefaultClusteringService secondService = new DefaultClusteringService(configuration);
    secondService.start(null);

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(2));

    firstService.stop();
    assertThat(activeEntity.getConnectedClients().size(), is(1));

    secondService.stop();
    assertThat(activeEntity.getConnectedClients().size(), is(0));
  }

  /**
   * Ensures a second client specifying auto-create can start {@link DefaultClusteringService} while the
   * creator is not connected.
   */
  @Test
  public void testStartStopAutoCreateTwiceB() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .build();
    DefaultClusteringService firstService = new DefaultClusteringService(configuration);
    firstService.start(null);
    firstService.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));

    DefaultClusteringService secondService = new DefaultClusteringService(configuration);
    secondService.start(null);

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));

    secondService.stop();
    assertThat(activeEntity.getConnectedClients().size(), is(0));
  }

  @Test
  public void testStartForMaintenanceAutoStart() throws Exception {
    URI clusterUri = URI.create(CLUSTER_URI_BASE + "my-application");
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(clusterUri)
            .autoCreate(true)
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.startForMaintenance(null);

    assertThat(UnitTestConnectionService.getConnectionProperties(clusterUri).size(), is(1));
    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(0));

    // startForMaintenance does **not** create an EhcacheActiveEntity

    service.stop();

    assertThat(UnitTestConnectionService.getConnectionProperties(clusterUri).size(), is(0));
  }

  @Test
  public void testStartForMaintenanceOtherAutoCreate() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .build();
    DefaultClusteringService createService = new DefaultClusteringService(configuration);
    createService.start(null);

    DefaultClusteringService maintenanceService = new DefaultClusteringService(configuration);
    try {
      maintenanceService.startForMaintenance(null);
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      // Expected
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));

    createService.stop();
    assertThat(activeEntity.getConnectedClients().size(), is(0));

    maintenanceService.stop();
    assertThat(activeEntity.getConnectedClients().size(), is(0));
  }

  @Test
  public void testStartForMaintenanceOtherCreated() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .build();
    DefaultClusteringService createService = new DefaultClusteringService(configuration);
    createService.start(null);
    createService.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);

    assertThat(activeEntity.getConnectedClients().size(), is(0));

    DefaultClusteringService maintenanceService = new DefaultClusteringService(configuration);
    maintenanceService.startForMaintenance(null);

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));

    // startForMaintenance does **not** establish a link with the EhcacheActiveEntity
    assertThat(activeEntity.getConnectedClients().size(), is(0));

    maintenanceService.stop();
    assertThat(activeEntity.getConnectedClients().size(), is(0));
  }

  /**
   * This test ensures that an auto-create client can't perform an auto-creation while a startForMaintenance client
   * is active.
   */
  @Test
  public void testStartForMaintenanceCreateInterlock() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .build();
    DefaultClusteringService maintenanceService = new DefaultClusteringService(configuration);
    maintenanceService.startForMaintenance(null);

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(0));

    DefaultClusteringService createService = new DefaultClusteringService(configuration);
    try {
      createService.start(null);
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getCause(), is(instanceOf(EhcacheEntityCreationException.class)));
    }

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(0));

    maintenanceService.stop();

    createService.start(null);

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));

    createService.stop();
    assertThat(activeEntity.getConnectedClients().size(), is(0));
  }

  @Test
  public void testStartForMaintenanceInterlock() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .build();
    DefaultClusteringService maintenanceService1 = new DefaultClusteringService(configuration);
    maintenanceService1.startForMaintenance(null);

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(0));

    DefaultClusteringService maintenanceService2 = new DefaultClusteringService(configuration);
    try {
      maintenanceService2.startForMaintenance(null);
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString(" acquire cluster-wide "));
    }

    maintenanceService1.stop();
  }

  @Test
  public void testStartForMaintenanceSequence() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .build();
    DefaultClusteringService maintenanceService1 = new DefaultClusteringService(configuration);
    maintenanceService1.startForMaintenance(null);
    maintenanceService1.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(0));

    DefaultClusteringService maintenanceService2 = new DefaultClusteringService(configuration);
    maintenanceService2.startForMaintenance(null);
    maintenanceService2.stop();

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(0));
  }

  @Test
  public void testBasicConfiguration() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService createService = new DefaultClusteringService(configuration);
    createService.start(null);

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));

    createService.stop();

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testBasicDestroyAll() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService createService = new DefaultClusteringService(configuration);
    createService.start(null);
    createService.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));

    try {
      createService.destroyAll();
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("Maintenance mode required"));
    }

    createService.startForMaintenance(null);

    createService.destroyAll();

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testGetServerStoreProxySharedAutoCreate() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetPool = "sharedPrimary";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(
        getClusteredCacheIdentifier(service, cacheAlias, storeConfiguration), storeConfiguration, Consistency.EVENTUAL);

    assertThat(serverStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(1));

    service.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));
  }

  @Test
  public void testGetServerStoreProxySharedNoAutoCreateNonExistent() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetPool = "sharedPrimary";
    ClusteringServiceConfiguration creationConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);
    creationService.stop();

    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    accessService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    try {
      accessService.getServerStoreProxy(
          getClusteredCacheIdentifier(accessService, cacheAlias, storeConfiguration), storeConfiguration, Consistency.EVENTUAL);
      fail("Expecting ClusteredStoreValidationException");
    } catch (ClusteredStoreValidationException e) {
      // Expected
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
  }

  @Test
  public void testGetServerStoreProxySharedNoAutoCreateExists() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetPool = "sharedPrimary";
    ClusteringServiceConfiguration creationConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> creationStoreConfig =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy creationServerStoreProxy = creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias, creationStoreConfig), creationStoreConfig, Consistency.EVENTUAL);
    assertThat(creationServerStoreProxy.getCacheId(), is(cacheAlias));

    creationService.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));


    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    accessService.start(null);

    Store.Configuration<Long, String> accessStoreConfiguration =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy accessServerStoreProxy = accessService.getServerStoreProxy(
        getClusteredCacheIdentifier(accessService, cacheAlias, accessStoreConfiguration), accessStoreConfiguration, Consistency.EVENTUAL);
    assertThat(accessServerStoreProxy.getCacheId(), is(cacheAlias));

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(1));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));
  }

  /**
   * Ensures that two clients using auto-create can gain access to the same {@code ServerStore}.
   */
  @Test
  public void testGetServerStoreProxySharedAutoCreateTwice() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetPool = "sharedPrimary";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService firstService = new DefaultClusteringService(configuration);
    firstService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> firstSharedStoreConfig =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy firstServerStoreProxy = firstService.getServerStoreProxy(
        getClusteredCacheIdentifier(firstService, cacheAlias, firstSharedStoreConfig), firstSharedStoreConfig, Consistency.EVENTUAL);
    assertThat(firstServerStoreProxy.getCacheId(), is(cacheAlias));

    DefaultClusteringService secondService = new DefaultClusteringService(configuration);
    secondService.start(null);

    Store.Configuration<Long, String> secondSharedStoreConfig =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy secondServerStoreProxy = secondService.getServerStoreProxy(
        getClusteredCacheIdentifier(firstService, cacheAlias, secondSharedStoreConfig), secondSharedStoreConfig, Consistency.EVENTUAL);
    assertThat(secondServerStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(2));
    for (Set<String> storeIds : activeEntity.getConnectedClients().values()) {
      assertThat(storeIds, containsInAnyOrder(cacheAlias));
    }
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(2));

    firstService.stop();

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(1));

    secondService.stop();

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));
  }

  @Test
  public void testReleaseServerStoreProxyShared() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetPool = "sharedPrimary";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(configuration);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy serverStoreProxy = creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias, storeConfiguration), storeConfiguration, Consistency.EVENTUAL);
    assertThat(serverStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(1));

    creationService.releaseServerStoreProxy(serverStoreProxy);

    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));

    try {
      creationService.releaseServerStoreProxy(serverStoreProxy);
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString(" not in use "));
    }

    creationService.stop();
  }

  @Test
  public void testGetServerStoreProxyFixedAutoCreate() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getFixedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(
        getClusteredCacheIdentifier(service, cacheAlias, storeConfiguration), storeConfiguration, Consistency.EVENTUAL);

    assertThat(serverStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(1));

    service.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));
  }

  @Test
  public void testGetServerStoreProxyFixedNoAutoCreateNonExistent() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration creationConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);
    creationService.stop();

    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    accessService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getFixedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    try {
      accessService.getServerStoreProxy(
          getClusteredCacheIdentifier(accessService, cacheAlias, storeConfiguration), storeConfiguration, Consistency.EVENTUAL);
      fail("Expecting ClusteredStoreValidationException");
    } catch (ClusteredStoreValidationException e) {
      // Expected
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
  }

  @Test
  public void testGetServerStoreProxyFixedNoAutoCreateExists() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration creationConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> creationStoreConfig =
        getFixedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy creationServerStoreProxy = creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias, creationStoreConfig), creationStoreConfig, Consistency.EVENTUAL);
    assertThat(creationServerStoreProxy.getCacheId(), is(cacheAlias));

    creationService.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));

    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    accessService.start(null);

    Store.Configuration<Long, String> accessStoreConfiguration =
        getFixedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy accessServerStoreProxy = accessService.getServerStoreProxy(
        getClusteredCacheIdentifier(accessService, cacheAlias, accessStoreConfiguration), accessStoreConfiguration, Consistency.EVENTUAL);
    assertThat(accessServerStoreProxy.getCacheId(), is(cacheAlias));

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(1));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));
  }

  /**
   * Ensures that two clients using auto-create can gain access to the same {@code ServerStore}.
   */
  @Test
  public void testGetServerStoreProxyFixedAutoCreateTwice() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService firstService = new DefaultClusteringService(configuration);
    firstService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> firstSharedStoreConfig =
        getFixedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy firstServerStoreProxy = firstService.getServerStoreProxy(
        getClusteredCacheIdentifier(firstService, cacheAlias, firstSharedStoreConfig), firstSharedStoreConfig, Consistency.EVENTUAL);
    assertThat(firstServerStoreProxy.getCacheId(), is(cacheAlias));

    DefaultClusteringService secondService = new DefaultClusteringService(configuration);
    secondService.start(null);

    Store.Configuration<Long, String> secondSharedStoreConfig =
        getFixedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy secondServerStoreProxy = secondService.getServerStoreProxy(
        getClusteredCacheIdentifier(firstService, cacheAlias, secondSharedStoreConfig), secondSharedStoreConfig, Consistency.EVENTUAL);
    assertThat(secondServerStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(2));
    for (Set<String> storeIds : activeEntity.getConnectedClients().values()) {
      assertThat(storeIds, containsInAnyOrder(cacheAlias));
    }
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(2));

    firstService.stop();

    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(1));

    secondService.stop();

    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));
  }

  @Test
  public void testReleaseServerStoreProxyFixed() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(configuration);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getFixedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy serverStoreProxy = creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias, storeConfiguration), storeConfiguration, Consistency.EVENTUAL);
    assertThat(serverStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(1));

    creationService.releaseServerStoreProxy(serverStoreProxy);

    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));

    try {
      creationService.releaseServerStoreProxy(serverStoreProxy);
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString(" not in use "));
    }

    creationService.stop();
  }

  @Test
  public void testReleaseServerStoreProxyNonExistent() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(configuration);
    creationService.start(null);

    try {
      creationService.releaseServerStoreProxy(mock(ServerStoreProxy.class));
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString(" does not exist"));
    }

    creationService.stop();
  }

  @Test
  public void testGetServerStoreProxySharedDestroy() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetPool = "sharedPrimary";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(configuration);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy serverStoreProxy = creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias, storeConfiguration), storeConfiguration, Consistency.EVENTUAL);
    assertThat(serverStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(1));

    try {
      creationService.destroy(cacheAlias);
      fail("Expecting CachePersistenceException");
    } catch (CachePersistenceException e) {
      assertThat(e.getMessage(), containsString(" in use by "));
    }

    creationService.releaseServerStoreProxy(serverStoreProxy);
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));

    creationService.destroy(cacheAlias);

    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));

    creationService.stop();
  }

  @Test
  public void testGetServerStoreProxyFixedDestroy() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(configuration);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getFixedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy serverStoreProxy = creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias, storeConfiguration), storeConfiguration, Consistency.EVENTUAL);
    assertThat(serverStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(1));

    try {
      creationService.destroy(cacheAlias);
      fail("Expecting CachePersistenceException");
    } catch (CachePersistenceException e) {
      assertThat(e.getMessage(), containsString(" in use by "));
    }

    creationService.releaseServerStoreProxy(serverStoreProxy);
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));

    creationService.destroy(cacheAlias);

    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));

    creationService.stop();
  }

  @Test
  public void testFullDestroyAll() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService createService = new DefaultClusteringService(configuration);
    createService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());

    Store.Configuration<Long, String> sharedStoreConfiguration =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, String.class);

    ServerStoreProxy sharedProxy = createService.getServerStoreProxy(
        getClusteredCacheIdentifier(createService, "sharedCache", sharedStoreConfiguration), sharedStoreConfiguration, Consistency.EVENTUAL);
    assertThat(sharedProxy.getCacheId(), is("sharedCache"));

    Store.Configuration<Long, String> storeConfiguration =
        getFixedStoreConfig("serverResource2", serializationProvider, Long.class, String.class);

    ServerStoreProxy fixedProxy = createService.getServerStoreProxy(
        getClusteredCacheIdentifier(createService, "fixedCache", storeConfiguration), storeConfiguration, Consistency.EVENTUAL);
    assertThat(fixedProxy.getCacheId(), is("fixedCache"));

    createService.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("fixedCache"));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder("sharedCache", "fixedCache"));

    try {
      createService.destroyAll();
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("Maintenance mode required"));
    }

    createService.startForMaintenance(null);

    createService.destroyAll();

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testStartNoAutoCreateThenAutoCreate() throws Exception {
    ClusteringServiceConfiguration creationConfigBad =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationServiceBad = new DefaultClusteringService(creationConfigBad);

    try {
      creationServiceBad.start(null);
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      // Expected
    }

    List<ObservableEhcacheActiveEntity> activeEntitiesBad = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntitiesBad.size(), is(0));

    ClusteringServiceConfiguration creationConfigGood =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate(true)
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationServiceGood = new DefaultClusteringService(creationConfigGood);

    creationServiceGood.start(null);
  }

  @Test
  public void testStoreValidation_autoCreateConfigGood_autoCreateConfigBad() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration config =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate(true)
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(config);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());

    Store.Configuration<Long, String> createStoreConfig =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, String.class);

    ClusteredCacheIdentifier clusteredCacheIdentifier = getClusteredCacheIdentifier(creationService, cacheAlias, createStoreConfig);

    creationService.getServerStoreProxy(clusteredCacheIdentifier, createStoreConfig, Consistency.EVENTUAL);

    creationService.stop();

    DefaultClusteringService accessService = new DefaultClusteringService(config);
    accessService.start(null);

    Store.Configuration<Long, Long> accessStoreConfigBad =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, Long.class);//ValueType is invalid

    try {
      accessService.getServerStoreProxy(clusteredCacheIdentifier, accessStoreConfigBad, Consistency.EVENTUAL);
      fail("Expecting ClusteredStoreValidationException");
    } catch(ClusteredStoreValidationException e) {
      // Expected
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(),is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias), is(Matchers.<ClientDescriptor>empty()));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias), is(Matchers.<ClientDescriptor>empty()));
  }

  @Test
  public void testStoreValidation_autoCreateConfigGood_autoCreateConfigGood() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration config =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate(true)
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(config);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());

    Store.Configuration<Long, String> storeConfig =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, String.class);

    ClusteredCacheIdentifier clusteredCacheIdentifier = getClusteredCacheIdentifier(creationService, cacheAlias, storeConfig);

    creationService.getServerStoreProxy(clusteredCacheIdentifier, storeConfig, Consistency.EVENTUAL);

    creationService.stop();

    DefaultClusteringService accessService = new DefaultClusteringService(config);
    accessService.start(null);

    accessService.getServerStoreProxy(clusteredCacheIdentifier, storeConfig, Consistency.EVENTUAL);

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(),is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(1));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));
  }

  @Test
  public void testStoreValidation_autoCreateConfigBad() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration config =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate(true)
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(config);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());

    Store.Configuration<Long, String> storeConfig =
        getSharedStoreConfig("fixedPrimary", serializationProvider, Long.class, String.class);

    ClusteredCacheIdentifier clusteredCacheIdentifier = getClusteredCacheIdentifier(creationService, cacheAlias, storeConfig);

    try {
      creationService.getServerStoreProxy(clusteredCacheIdentifier, storeConfig, Consistency.EVENTUAL);
      fail("Expecting ClusteredStoreCreationException");
    } catch(ClusteredStoreCreationException e) {
      //Expected
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores().size(), is(0));
    assertThat(activeEntity.getInUseStores().size(), is(0));

    creationService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores().size(), is(0));
    assertThat(activeEntity.getInUseStores().size(), is(0));
  }

  @Test
  public void testStoreValidation_autoCreateConfigGood_noAutoCreateConfigBad() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration autoConfig =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate(true)
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(autoConfig);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());

    Store.Configuration<Long, String> creationStoreConfig =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, String.class);

    ClusteredCacheIdentifier clusteredCacheIdentifier = getClusteredCacheIdentifier(creationService, cacheAlias, creationStoreConfig);

    creationService.getServerStoreProxy(clusteredCacheIdentifier, creationStoreConfig, Consistency.EVENTUAL);

    ClusteringServiceConfiguration noAutoConfig =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
        .build();

    DefaultClusteringService accessService = new DefaultClusteringService(noAutoConfig);
    accessService.start(null);

    Store.Configuration<Long, Long> accessStoreConfig =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, Long.class);

    try {
      accessService.getServerStoreProxy(clusteredCacheIdentifier, accessStoreConfig, Consistency.EVENTUAL);
      fail("Expecting ClusteredStoreValidationException");
    } catch(ClusteredStoreValidationException e) {
      //Expected
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));

    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds().size(), is(0));
    assertThat(activeEntity.getConnectedClients().size(), is(2));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(1));

    creationService.stop();
    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds().size(), is(0));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));
  }

  @Test
  public void testStoreValidation_autoCreateConfigGood_noAutoCreateConfigGood() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration autoConfig =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate(true)
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(autoConfig);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());

    Store.Configuration<Long, String> storeConfig =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, String.class);

    ClusteredCacheIdentifier clusteredCacheIdentifier = getClusteredCacheIdentifier(creationService, cacheAlias, storeConfig);

    creationService.getServerStoreProxy(clusteredCacheIdentifier, storeConfig, Consistency.EVENTUAL);


    ClusteringServiceConfiguration noAutoConfig =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
        .build();

    DefaultClusteringService accessService = new DefaultClusteringService(noAutoConfig);
    accessService.start(null);

    accessService.getServerStoreProxy(clusteredCacheIdentifier, storeConfig, Consistency.EVENTUAL);

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));

    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds().size(), is(0));
    assertThat(activeEntity.getConnectedClients().size(), is(2));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(2));//2 clients connected

    creationService.stop();
    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds().size(), is(0));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias).size(), is(0));
  }

  @Test
  public void testStoreValidation_MismatchedPoolTypes_ConfiguredFixedValidateShared() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration creationConfig =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate(true)
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> createStoreConfig =
        getFixedStoreConfig("serverResource1", serializationProvider, Long.class, String.class);

    creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, "cacheAlias", createStoreConfig), createStoreConfig, Consistency.EVENTUAL);

    creationService.stop();

    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    accessService.start(null);

    Store.Configuration accessStoreConfig =
        getSharedStoreConfig("serverResource1", serializationProvider, Long.class, String.class);

    try {
      accessService.getServerStoreProxy(
          getClusteredCacheIdentifier(accessService, cacheAlias, accessStoreConfig), accessStoreConfig, Consistency.EVENTUAL);
      fail("Expecting ClusteredStoreValidationException");
    } catch (ClusteredStoreValidationException e) {
      // Expected
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias), is(Matchers.<ClientDescriptor>empty()));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias), is(Matchers.<ClientDescriptor>empty()));
  }

  @Test
  public void testStoreValidation_MismatchedPoolTypes_ConfiguredSharedValidateFixed() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration creationConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> createStoreConfig =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, String.class);

    creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias, createStoreConfig), createStoreConfig, Consistency.EVENTUAL);

    creationService.stop();

    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate(true)
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 16, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 16, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 32, MemoryUnit.MB)
            .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    accessService.start(null);

    Store.Configuration<Long, String> accessStoreConfig =
        getFixedStoreConfig("defaultResource", serializationProvider, Long.class, String.class);

    try {
      accessService.getServerStoreProxy(
          getClusteredCacheIdentifier(accessService, cacheAlias, accessStoreConfig), accessStoreConfig, Consistency.EVENTUAL);
      fail("Expecting ClusteredStoreValidationException");
    } catch (ClusteredStoreValidationException e) {
      e.printStackTrace();
      // Expected
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getConnectedClients().values().iterator().next(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias), is(Matchers.<ClientDescriptor>empty()));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getInUseStores().get(cacheAlias), is(Matchers.<ClientDescriptor>empty()));
  }

  private <K, V> Store.Configuration<K, V> getSharedStoreConfig(
      String targetPool, DefaultSerializationProvider serializationProvider, Class<K> keyType, Class<V> valueType)
      throws org.ehcache.spi.serialization.UnsupportedTypeException {
    return new StoreConfigurationImpl<K, V>(
        CacheConfigurationBuilder.newCacheConfigurationBuilder(keyType, valueType,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .with(ClusteredResourcePoolBuilder.shared(targetPool)))
            .build(),
        StoreEventSourceConfiguration.DEFAULT_DISPATCHER_CONCURRENCY,
        serializationProvider.createKeySerializer(keyType, getClass().getClassLoader()),
        serializationProvider.createValueSerializer(valueType, getClass().getClassLoader()));
  }

  private <K, V> Store.Configuration<K, V> getFixedStoreConfig(
      String targetResource, DefaultSerializationProvider serializationProvider, Class<K> keyType, Class<V> valueType)
      throws org.ehcache.spi.serialization.UnsupportedTypeException {
    return new StoreConfigurationImpl<K, V>(
        CacheConfigurationBuilder.newCacheConfigurationBuilder(keyType, valueType,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .with(ClusteredResourcePoolBuilder.fixed(targetResource, 8, MemoryUnit.MB)))
            .build(),
        StoreEventSourceConfiguration.DEFAULT_DISPATCHER_CONCURRENCY,
        serializationProvider.createKeySerializer(keyType, getClass().getClassLoader()),
        serializationProvider.createValueSerializer(valueType, getClass().getClassLoader()));
  }

  private ClusteredCacheIdentifier getClusteredCacheIdentifier(
      DefaultClusteringService service, String cacheAlias, Store.Configuration<Long, String> storeConfiguration)
      throws CachePersistenceException {

    for (ResourceType<?> resourceType : ClusteredResourceType.Types.values()) {
      ResourcePool resourcePool = storeConfiguration.getResourcePools().getPoolForResource(resourceType);
      if (resourcePool != null) {
        ClusteredCacheIdentifier clusteredCacheIdentifier =
            ServiceLocator.findSingletonAmongst(ClusteredCacheIdentifier.class,
                service.getPersistenceSpaceIdentifier(cacheAlias, null));
        if (clusteredCacheIdentifier != null) {
          return clusteredCacheIdentifier;
        }
      }
    }
    throw new AssertionError("ClusteredCacheIdentifier not available for configuration");
  }

  @Test
  public void testGetServerStoreProxyReturnsEventualStore() throws Exception {
    String entityIdentifier = "my-application";
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(
            URI.create(CLUSTER_URI_BASE + entityIdentifier),
            true,
            null,
            Collections.<String, ClusteringServiceConfiguration.PoolDefinition>emptyMap());
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    ResourcePools resourcePools = mock(ResourcePools.class);
    Store.Configuration storeConfig = mock(Store.Configuration.class);
    ClusteringService.ClusteredCacheIdentifier cacheIdentifier = mock(ClusteringService.ClusteredCacheIdentifier.class);
    when(storeConfig.getResourcePools()).thenReturn(resourcePools);
    when(resourcePools.getPoolForResource(eq(FIXED))).thenReturn(new FixedClusteredResourcePoolImpl("serverResource1", 1L, MemoryUnit.MB));
    when(storeConfig.getKeyType()).thenReturn(String.class);
    when(storeConfig.getValueType()).thenReturn(Object.class);
    when(cacheIdentifier.getId()).thenReturn("my-cache");

    ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(cacheIdentifier, storeConfig, Consistency.EVENTUAL);
    assertThat(serverStoreProxy, instanceOf(EventualServerStoreProxy.class));
  }

  @Test
  public void testGetServerStoreProxyReturnsEventualStoreByDefault() throws Exception {
    String entityIdentifier = "my-application";
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(
            URI.create(CLUSTER_URI_BASE + entityIdentifier),
            true,
            null,
            Collections.<String, ClusteringServiceConfiguration.PoolDefinition>emptyMap());
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    ResourcePools resourcePools = mock(ResourcePools.class);
    Store.Configuration storeConfig = mock(Store.Configuration.class);
    ClusteringService.ClusteredCacheIdentifier cacheIdentifier = mock(ClusteringService.ClusteredCacheIdentifier.class);
    when(storeConfig.getResourcePools()).thenReturn(resourcePools);
    when(resourcePools.getPoolForResource(eq(FIXED))).thenReturn(new FixedClusteredResourcePoolImpl("serverResource1", 1L, MemoryUnit.MB));
    when(storeConfig.getKeyType()).thenReturn(String.class);
    when(storeConfig.getValueType()).thenReturn(Object.class);
    when(cacheIdentifier.getId()).thenReturn("my-cache");

    ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(cacheIdentifier, storeConfig, Consistency.EVENTUAL);
    assertThat(serverStoreProxy, instanceOf(EventualServerStoreProxy.class));
  }

  @Test
  public void testGetServerStoreProxyReturnsStrongStore() throws Exception {
    String entityIdentifier = "my-application";
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(
            URI.create(CLUSTER_URI_BASE + entityIdentifier),
            true,
            null,
            Collections.<String, ClusteringServiceConfiguration.PoolDefinition>emptyMap());
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    ResourcePools resourcePools = mock(ResourcePools.class);
    Store.Configuration storeConfig = mock(Store.Configuration.class);
    ClusteringService.ClusteredCacheIdentifier cacheIdentifier = mock(ClusteringService.ClusteredCacheIdentifier.class);
    when(storeConfig.getResourcePools()).thenReturn(resourcePools);
    when(resourcePools.getPoolForResource(eq(FIXED))).thenReturn(new FixedClusteredResourcePoolImpl("serverResource1", 1L, MemoryUnit.MB));
    when(storeConfig.getKeyType()).thenReturn(String.class);
    when(storeConfig.getValueType()).thenReturn(Object.class);
    when(cacheIdentifier.getId()).thenReturn("my-cache");

    ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(cacheIdentifier, storeConfig, Consistency.STRONG);
    assertThat(serverStoreProxy, instanceOf(StrongServerStoreProxy.class));
  }
}