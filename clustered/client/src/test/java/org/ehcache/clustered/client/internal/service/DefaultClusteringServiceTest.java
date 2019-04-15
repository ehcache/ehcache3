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
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityFactory;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService.PassthroughServerBuilder;
import org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntityService;
import org.ehcache.clustered.client.internal.store.EventualServerStoreProxy;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.ServerCallback;
import org.ehcache.clustered.client.internal.store.StrongServerStoreProxy;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.client.service.ClusteringService.ClusteredCacheIdentifier;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerStoreConfigurationException;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.ObservableEhcacheServerEntityService;
import org.ehcache.clustered.server.ObservableEhcacheServerEntityService.ObservableEhcacheActiveEntity;
import org.ehcache.clustered.server.store.ObservableClusterTierServerEntityService;
import org.ehcache.clustered.server.store.ObservableClusterTierServerEntityService.ObservableClusterTierActiveEntity;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.store.StoreEventSourceConfiguration;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.service.MaintainableService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.exception.EntityNotFoundException;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.ehcache.clustered.client.config.ClusteredResourceType.Types.DEDICATED;
import static org.ehcache.clustered.client.config.ClusteredResourceType.Types.SHARED;
import static org.ehcache.clustered.client.internal.service.TestServiceProvider.providerContaining;
import static org.ehcache.config.ResourceType.Core.DISK;
import static org.ehcache.config.ResourceType.Core.HEAP;
import static org.ehcache.config.ResourceType.Core.OFFHEAP;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultClusteringServiceTest {

  private static final String CLUSTER_URI_BASE = "terracotta://example.com:9540/";
  private ObservableEhcacheServerEntityService observableEhcacheServerEntityService;
  private ObservableClusterTierServerEntityService observableClusterTierServerEntityService;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void definePassthroughServer() throws Exception {
    observableEhcacheServerEntityService = new ObservableEhcacheServerEntityService();
    observableClusterTierServerEntityService = new ObservableClusterTierServerEntityService();
    UnitTestConnectionService.add(CLUSTER_URI_BASE,
        new PassthroughServerBuilder()
            .serverEntityService(observableEhcacheServerEntityService)
            .clientEntityService(new ClusterTierManagerClientEntityService())
            .serverEntityService(observableClusterTierServerEntityService)
            .clientEntityService(new ClusterTierClientEntityService())
            .serverEntityService(new VoltronReadWriteLockServerEntityService())
            .clientEntityService(new VoltronReadWriteLockEntityClientService())
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
            .autoCreate()
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);

    assertThat(service.handlesResourceType(DISK), is(false));
    assertThat(service.handlesResourceType(HEAP), is(false));
    assertThat(service.handlesResourceType(OFFHEAP), is(false));
    assertThat(service.handlesResourceType(DEDICATED), is(true));
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
            .autoCreate()
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);

    PersistableResourceService.PersistenceSpaceIdentifier<?> spaceIdentifier = service.getPersistenceSpaceIdentifier("cacheAlias", null);
    assertThat(spaceIdentifier, is(instanceOf(ClusteredCacheIdentifier.class)));
    assertThat(((ClusteredCacheIdentifier)spaceIdentifier).getId(), is("cacheAlias"));
    assertThat(service.getPersistenceSpaceIdentifier("cacheAlias", null), sameInstance(spaceIdentifier));
  }

  @Test
  public void testCreate() throws Exception {
    CacheConfigurationBuilder<Long, String> configBuilder =
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .with(ClusteredResourcePoolBuilder.clusteredShared("primary")));
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);

    PersistableResourceService.PersistenceSpaceIdentifier<?> spaceIdentifier = service.getPersistenceSpaceIdentifier("cacheAlias", configBuilder
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
            true, new ServerSideConfiguration(Collections.<String, Pool>emptyMap()));
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
            .autoCreate()
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    assertThat(service.isConnected(), is(false));
    service.start(null);
    assertThat(service.isConnected(), is(true));

    assertThat(UnitTestConnectionService.getConnectionProperties(clusterUri).size(), is(1));
    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
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
            .autoCreate()
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
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
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
            .autoCreate()
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
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));

    secondService.stop();
    assertThat(activeEntity.getConnectedClients().size(), is(0));
  }

  @Test
  public void testStartForMaintenanceAutoStart() throws Exception {
    URI clusterUri = URI.create(CLUSTER_URI_BASE + "my-application");
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(clusterUri)
            .autoCreate()
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    assertThat(service.isConnected(), is(false));
    service.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);
    assertThat(service.isConnected(), is(true));

    assertThat(UnitTestConnectionService.getConnectionProperties(clusterUri).size(), is(1));
    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(0));

    // startForMaintenance does **not** create an ClusterTierManagerActiveEntity

    service.stop();

    assertThat(UnitTestConnectionService.getConnectionProperties(clusterUri).size(), is(0));
  }

  @Test
  public void testStartForMaintenanceOtherAutoCreate() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .build();
    DefaultClusteringService createService = new DefaultClusteringService(configuration);
    createService.start(null);

    DefaultClusteringService maintenanceService = new DefaultClusteringService(configuration);
    try {
      maintenanceService.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      // Expected
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
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
            .autoCreate()
            .build();
    DefaultClusteringService createService = new DefaultClusteringService(configuration);
    createService.start(null);
    createService.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);

    assertThat(activeEntity.getConnectedClients().size(), is(0));

    DefaultClusteringService maintenanceService = new DefaultClusteringService(configuration);
    maintenanceService.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    // startForMaintenance does **not** establish a link with the ClusterTierManagerActiveEntity
    assertThat(activeEntity.getConnectedClients().size(), is(0));

    maintenanceService.stop();
    assertThat(activeEntity.getConnectedClients().size(), is(0));
  }

  @Test
  public void testMultipleAutoCreateClientsRunConcurrently() throws InterruptedException, ExecutionException {
    final ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .build();

    Callable<DefaultClusteringService> task = () -> {
      DefaultClusteringService service = new DefaultClusteringService(configuration);
      service.start(null);
      return service;
    };

    ExecutorService executor = Executors.newCachedThreadPool();
    try {
      List<Future<DefaultClusteringService>> results = executor.invokeAll(Collections.nCopies(4, task), 5, TimeUnit.MINUTES);
      for (Future<DefaultClusteringService> result : results) {
        assertThat(result.isDone(), is(true));
      }
      for (Future<DefaultClusteringService> result : results) {
        result.get().stop();
      }
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testStartForMaintenanceInterlock() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .build();
    DefaultClusteringService maintenanceService1 = new DefaultClusteringService(configuration);
    maintenanceService1.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(0));

    DefaultClusteringService maintenanceService2 = new DefaultClusteringService(configuration);
    try {
      maintenanceService2.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);
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
            .autoCreate()
            .build();
    DefaultClusteringService maintenanceService1 = new DefaultClusteringService(configuration);
    maintenanceService1.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);
    maintenanceService1.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(0));

    DefaultClusteringService maintenanceService2 = new DefaultClusteringService(configuration);
    maintenanceService2.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);
    maintenanceService2.stop();

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(0));
  }

  @Test
  public void testBasicConfiguration() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService createService = new DefaultClusteringService(configuration);
    createService.start(null);

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));

    createService.stop();

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testGetServerStoreProxySharedAutoCreate() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetPool = "sharedPrimary";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(
        getClusteredCacheIdentifier(service, cacheAlias), storeConfiguration, Consistency.EVENTUAL, mock(ServerCallback.class));

    assertThat(serverStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(1));

    service.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());
  }

  @Test
  public void testGetServerStoreProxySharedNoAutoCreateNonExistent() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetPool = "sharedPrimary";
    ClusteringServiceConfiguration creationConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);
    creationService.stop();

    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .expecting()
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    accessService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    try {
      accessService.getServerStoreProxy(
          getClusteredCacheIdentifier(accessService, cacheAlias), storeConfiguration, Consistency.EVENTUAL, mock(ServerCallback.class));
      fail("Expecting CachePersistenceException");
    } catch (CachePersistenceException e) {
      assertThat(e.getMessage(), containsString(" does not exist"));
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testGetServerStoreProxySharedNoAutoCreateExists() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetPool = "sharedPrimary";
    ClusteringServiceConfiguration creationConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> creationStoreConfig =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy creationServerStoreProxy = creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias), creationStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(creationServerStoreProxy.getCacheId(), is(cacheAlias));

    creationService.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());


    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .expecting()
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    accessService.start(null);

    Store.Configuration<Long, String> accessStoreConfiguration =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy accessServerStoreProxy = accessService.getServerStoreProxy(
        getClusteredCacheIdentifier(accessService, cacheAlias), accessStoreConfiguration, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(accessServerStoreProxy.getCacheId(), is(cacheAlias));

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(1));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());
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
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService firstService = new DefaultClusteringService(configuration);
    firstService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> firstSharedStoreConfig =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy firstServerStoreProxy = firstService.getServerStoreProxy(
        getClusteredCacheIdentifier(firstService, cacheAlias), firstSharedStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(firstServerStoreProxy.getCacheId(), is(cacheAlias));

    DefaultClusteringService secondService = new DefaultClusteringService(configuration);
    secondService.start(null);

    Store.Configuration<Long, String> secondSharedStoreConfig =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy secondServerStoreProxy = secondService.getServerStoreProxy(
        getClusteredCacheIdentifier(secondService, cacheAlias), secondSharedStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(secondServerStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(2));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(2));

    firstService.stop();

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(1));

    secondService.stop();

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder(targetPool, "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());
  }

  @Test
  public void testReleaseServerStoreProxyShared() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetPool = "sharedPrimary";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(configuration);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy serverStoreProxy = creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias), storeConfiguration, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(serverStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(1));

    creationService.releaseServerStoreProxy(serverStoreProxy, false);

    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());

    try {
      creationService.releaseServerStoreProxy(serverStoreProxy, false);
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("Endpoint closed"));
    }

    creationService.stop();
  }

  @Test
  public void testGetServerStoreProxyDedicatedAutoCreate() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getDedicatedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(
        getClusteredCacheIdentifier(service, cacheAlias), storeConfiguration, Consistency.EVENTUAL, mock(ServerCallback.class));

    assertThat(serverStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(1));

    service.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());
  }

  @Test
  public void testGetServerStoreProxyDedicatedNoAutoCreateNonExistent() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration creationConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);
    creationService.stop();

    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .expecting()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    accessService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getDedicatedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    try {
      accessService.getServerStoreProxy(
          getClusteredCacheIdentifier(accessService, cacheAlias), storeConfiguration, Consistency.EVENTUAL, mock(ServerCallback.class));
      fail("Expecting CachePersistenceException");
    } catch (CachePersistenceException e) {
      assertThat(e.getMessage(), containsString(" does not exist"));
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testGetServerStoreProxyDedicatedNoAutoCreateExists() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration creationConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> creationStoreConfig =
        getDedicatedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy creationServerStoreProxy = creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias), creationStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(creationServerStoreProxy.getCacheId(), is(cacheAlias));

    creationService.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());

    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .expecting()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    accessService.start(null);

    Store.Configuration<Long, String> accessStoreConfiguration =
        getDedicatedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy accessServerStoreProxy = accessService.getServerStoreProxy(
        getClusteredCacheIdentifier(accessService, cacheAlias), accessStoreConfiguration, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(accessServerStoreProxy.getCacheId(), is(cacheAlias));

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(1));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());
  }

  /**
   * Ensures that two clients using auto-create can gain access to the same {@code ServerStore}.
   */
  @Test
  public void testGetServerStoreProxyDedicatedAutoCreateTwice() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService firstService = new DefaultClusteringService(configuration);
    firstService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> firstSharedStoreConfig =
        getDedicatedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy firstServerStoreProxy = firstService.getServerStoreProxy(
        getClusteredCacheIdentifier(firstService, cacheAlias), firstSharedStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(firstServerStoreProxy.getCacheId(), is(cacheAlias));

    DefaultClusteringService secondService = new DefaultClusteringService(configuration);
    secondService.start(null);

    Store.Configuration<Long, String> secondSharedStoreConfig =
        getDedicatedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy secondServerStoreProxy = secondService.getServerStoreProxy(
        getClusteredCacheIdentifier(secondService, cacheAlias), secondSharedStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(secondServerStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(2));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(2));

    firstService.stop();

    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(1));

    secondService.stop();

    assertThat(activeEntity.getSharedResourcePoolIds(),
        containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());
  }

  @Test
  public void testReleaseServerStoreProxyDedicated() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(configuration);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getDedicatedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy serverStoreProxy = creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias), storeConfiguration, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(serverStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients(), not(empty()));

    creationService.releaseServerStoreProxy(serverStoreProxy, false);

    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());

    try {
      creationService.releaseServerStoreProxy(serverStoreProxy, false);
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("Endpoint closed"));
    }

    creationService.stop();
  }

  @Test
  public void testGetServerStoreProxySharedDestroy() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetPool = "sharedPrimary";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool(targetPool, 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(configuration);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getSharedStoreConfig(targetPool, serializationProvider, Long.class, String.class);

    ServerStoreProxy serverStoreProxy = creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias), storeConfiguration, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(serverStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(1));

    try {
      creationService.destroy(cacheAlias);
      fail("Expecting CachePersistenceException");
    } catch (CachePersistenceException e) {
      assertThat(getRootCause(e).getMessage(), containsString(" in use by "));
    }

    creationService.releaseServerStoreProxy(serverStoreProxy, false);
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());

    creationService.destroy(cacheAlias);

    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));

    creationService.stop();
  }

  @Test
  public void testGetServerStoreProxyDedicatedDestroy() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(configuration);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> storeConfiguration =
        getDedicatedStoreConfig(targetResource, serializationProvider, Long.class, String.class);

    ServerStoreProxy serverStoreProxy = creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias), storeConfiguration, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(serverStoreProxy.getCacheId(), is(cacheAlias));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(1));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients(), not(empty()));

    try {
      creationService.destroy(cacheAlias);
      fail("Expecting CachePersistenceException");
    } catch (CachePersistenceException e) {
      assertThat(getRootCause(e).getMessage(), containsString(" in use by "));
    }

    creationService.releaseServerStoreProxy(serverStoreProxy, false);
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());

    creationService.destroy(cacheAlias);

    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));

    creationService.stop();
  }

  @Test
  public void testDestroyCantBeCalledIfStopped() throws Exception {
    String cacheAlias = "cacheAlias";
    String targetResource = "serverResource2";
    ClusteringServiceConfiguration configuration =
      ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate()
        .defaultServerResource("defaultResource")
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(configuration);

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(endsWith(" should be started to call destroy"));

    creationService.destroy(cacheAlias);
  }

  @Test
  public void testDestroyAllNoStores() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService createService = new DefaultClusteringService(configuration);
    createService.start(null);
    createService.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));

    try {
      createService.destroyAll();
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("Maintenance mode required"));
    }

    createService.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);

    createService.destroyAll();

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testDestroyAllWithStores() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService createService = new DefaultClusteringService(configuration);
    createService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());

    Store.Configuration<Long, String> sharedStoreConfiguration =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, String.class);

    ServerStoreProxy sharedProxy = createService.getServerStoreProxy(
        getClusteredCacheIdentifier(createService, "sharedCache"), sharedStoreConfiguration, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(sharedProxy.getCacheId(), is("sharedCache"));

    Store.Configuration<Long, String> storeConfiguration =
        getDedicatedStoreConfig("serverResource2", serializationProvider, Long.class, String.class);

    ServerStoreProxy dedicatedProxy = createService.getServerStoreProxy(
        getClusteredCacheIdentifier(createService, "dedicatedCache"), storeConfiguration, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(dedicatedProxy.getCacheId(), is("dedicatedCache"));

    createService.stop();

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache"));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), containsInAnyOrder("sharedCache", "dedicatedCache"));

    try {
      createService.destroyAll();
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("Maintenance mode required"));
    }

    createService.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);

    createService.destroyAll();

    activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is(nullValue()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testStartNoAutoCreateThenAutoCreate() throws Exception {
    ClusteringServiceConfiguration creationConfigBad =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .expecting()
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
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
        .autoCreate()
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationServiceGood = new DefaultClusteringService(creationConfigGood);

    creationServiceGood.start(null);
  }

  @Test
  public void testStoreValidation_autoCreateConfigGood_autoCreateConfigBad() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration config =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate()
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(config);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());

    Store.Configuration<Long, String> createStoreConfig =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, String.class);

    ClusteredCacheIdentifier clusteredCacheIdentifier = getClusteredCacheIdentifier(creationService, cacheAlias);

    creationService.getServerStoreProxy(clusteredCacheIdentifier, createStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));

    creationService.stop();

    DefaultClusteringService accessService = new DefaultClusteringService(config);
    accessService.start(null);

    Store.Configuration<Long, Long> accessStoreConfigBad =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, Long.class);//ValueType is invalid

    try {
      accessService.getServerStoreProxy(getClusteredCacheIdentifier(accessService, cacheAlias), accessStoreConfigBad, Consistency.EVENTUAL, mock(ServerCallback.class));
      fail("Expecting CachePersistenceException");
    } catch(CachePersistenceException e) {
      assertThat(getRootCause(e).getMessage(), containsString("Existing ServerStore configuration is not compatible with the desired configuration"));
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(),is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());
  }

  @Test
  public void testStoreValidation_autoCreateConfigGood_autoCreateConfigGood() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration config =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate()
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(config);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());

    Store.Configuration<Long, String> storeConfig =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, String.class);

    creationService.getServerStoreProxy(getClusteredCacheIdentifier(creationService, cacheAlias), storeConfig, Consistency.EVENTUAL, mock(ServerCallback.class));

    creationService.stop();

    DefaultClusteringService accessService = new DefaultClusteringService(config);
    accessService.start(null);

    accessService.getServerStoreProxy(getClusteredCacheIdentifier(accessService, cacheAlias), storeConfig, Consistency.EVENTUAL, mock(ServerCallback.class));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(),is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(1));

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());
  }

  @Test
  public void testStoreValidation_autoCreateConfigBad() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration config =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate()
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(config);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());

    Store.Configuration<Long, String> storeConfig =
        getSharedStoreConfig("dedicatedPrimary", serializationProvider, Long.class, String.class);

    ClusteredCacheIdentifier clusteredCacheIdentifier = getClusteredCacheIdentifier(creationService, cacheAlias);

    try {
      creationService.getServerStoreProxy(clusteredCacheIdentifier, storeConfig, Consistency.EVENTUAL, mock(ServerCallback.class));
      fail("Expecting CachePersistenceException");
    } catch(CachePersistenceException e) {
      //Expected
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));
    assertThat(activeEntity.getStores().size(), is(0));

    creationService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(activeEntity.getStores().size(), is(0));
  }

  @Test
  public void testStoreValidation_autoCreateConfigGood_noAutoCreateConfigBad() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration autoConfig =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate()
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(autoConfig);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());

    Store.Configuration<Long, String> creationStoreConfig =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, String.class);

    creationService.getServerStoreProxy(getClusteredCacheIdentifier(creationService, cacheAlias), creationStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));

    ClusteringServiceConfiguration noAutoConfig =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .expecting()
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
        .build();

    DefaultClusteringService accessService = new DefaultClusteringService(noAutoConfig);
    accessService.start(null);

    Store.Configuration<Long, Long> accessStoreConfig =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, Long.class);

    try {
      accessService.getServerStoreProxy(getClusteredCacheIdentifier(accessService, cacheAlias), accessStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));
      fail("Expecting CachePersistenceException");
    } catch(CachePersistenceException e) {
      assertThat(getRootCause(e).getMessage(), containsString("Existing ServerStore configuration is not compatible with the desired configuration"));
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));

    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds().size(), is(0));
    assertThat(activeEntity.getConnectedClients().size(), is(2));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(1));

    creationService.stop();
    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds().size(), is(0));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());
  }

  @Test
  public void testStoreValidation_autoCreateConfigGood_noAutoCreateConfigGood() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration autoConfig =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate()
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(autoConfig);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());

    Store.Configuration<Long, String> storeConfig =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, String.class);

    creationService.getServerStoreProxy(getClusteredCacheIdentifier(creationService, cacheAlias), storeConfig, Consistency.EVENTUAL, mock(ServerCallback.class));


    ClusteringServiceConfiguration noAutoConfig =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .expecting()
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
        .build();

    DefaultClusteringService accessService = new DefaultClusteringService(noAutoConfig);
    accessService.start(null);

    accessService.getServerStoreProxy(getClusteredCacheIdentifier(accessService, cacheAlias), storeConfig, Consistency.EVENTUAL, mock(ServerCallback.class));

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));

    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds().size(), is(0));
    assertThat(activeEntity.getConnectedClients().size(), is(2));
    assertThat(activeEntity.getStores(), containsInAnyOrder(cacheAlias));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients().size(), is(2));

    creationService.stop();
    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds().size(), is(0));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());
  }

  @Test
  public void testStoreValidation_MismatchedPoolTypes_ConfiguredDedicatedValidateShared() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration creationConfig =
    ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
        .autoCreate()
        .defaultServerResource("defaultResource")
        .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
        .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
        .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
        .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> createStoreConfig =
        getDedicatedStoreConfig("serverResource1", serializationProvider, Long.class, String.class);

    creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, "cacheAlias"), createStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));

    creationService.stop();

    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    accessService.start(null);

    Store.Configuration<Long, String> accessStoreConfig =
        getSharedStoreConfig("serverResource1", serializationProvider, Long.class, String.class);

    try {
      accessService.getServerStoreProxy(
          getClusteredCacheIdentifier(accessService, cacheAlias), accessStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));
      fail("Expecting CachePersistenceException");
    } catch (CachePersistenceException e) {
      assertThat(getRootCause(e).getMessage(), containsString("Existing ServerStore configuration is not compatible with the desired configuration"));
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().size(), is(1));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), containsInAnyOrder(cacheAlias));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());
  }

  @Test
  public void testStoreValidation_MismatchedPoolTypes_ConfiguredSharedValidateDedicated() throws Exception {
    String cacheAlias = "cacheAlias";
    ClusteringServiceConfiguration creationConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService creationService = new DefaultClusteringService(creationConfig);
    creationService.start(null);

    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(providerContaining());
    Store.Configuration<Long, String> createStoreConfig =
        getSharedStoreConfig("sharedPrimary", serializationProvider, Long.class, String.class);

    creationService.getServerStoreProxy(
        getClusteredCacheIdentifier(creationService, cacheAlias), createStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));

    creationService.stop();

    ClusteringServiceConfiguration accessConfig =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(CLUSTER_URI_BASE + "my-application"))
            .autoCreate()
            .defaultServerResource("defaultResource")
            .resourcePool("sharedPrimary", 2, MemoryUnit.MB, "serverResource1")
            .resourcePool("sharedSecondary", 2, MemoryUnit.MB, "serverResource2")
            .resourcePool("sharedTertiary", 4, MemoryUnit.MB)
            .build();
    DefaultClusteringService accessService = new DefaultClusteringService(accessConfig);
    accessService.start(null);

    Store.Configuration<Long, String> accessStoreConfig =
        getDedicatedStoreConfig("defaultResource", serializationProvider, Long.class, String.class);

    try {
      accessService.getServerStoreProxy(
          getClusteredCacheIdentifier(accessService, cacheAlias), accessStoreConfig, Consistency.EVENTUAL, mock(ServerCallback.class));
      fail("Expecting CachePersistenceException");
    } catch (CachePersistenceException e) {
      assertThat(getRootCause(e).getMessage(), containsString("Existing ServerStore configuration is not compatible with the desired configuration"));
    }

    List<ObservableEhcacheActiveEntity> activeEntities = observableEhcacheServerEntityService.getServedActiveEntities();
    assertThat(activeEntities.size(), is(1));
    ObservableEhcacheActiveEntity activeEntity = activeEntities.get(0);
    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(1));

    List<ObservableClusterTierActiveEntity> clusterTierActiveEntities = observableClusterTierServerEntityService.getServedActiveEntities();
    assertThat(clusterTierActiveEntities.size(), is(1));
    ObservableClusterTierActiveEntity clusterTierActiveEntity = clusterTierActiveEntities.get(0);
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());

    accessService.stop();

    assertThat(activeEntity.getDefaultServerResource(), is("defaultResource"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("sharedPrimary", "sharedSecondary", "sharedTertiary"));
    assertThat(activeEntity.getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().size(), is(0));
    assertThat(clusterTierActiveEntity.getConnectedClients(), empty());
  }

  private <K, V> Store.Configuration<K, V> getSharedStoreConfig(
      String targetPool, DefaultSerializationProvider serializationProvider, Class<K> keyType, Class<V> valueType)
      throws org.ehcache.spi.serialization.UnsupportedTypeException {
    return new StoreConfigurationImpl<>(
      CacheConfigurationBuilder.newCacheConfigurationBuilder(keyType, valueType,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredShared(targetPool)))
        .build(),
      StoreEventSourceConfiguration.DEFAULT_DISPATCHER_CONCURRENCY,
      serializationProvider.createKeySerializer(keyType, getClass().getClassLoader()),
      serializationProvider.createValueSerializer(valueType, getClass().getClassLoader()));
  }

  private <K, V> Store.Configuration<K, V> getDedicatedStoreConfig(
      String targetResource, DefaultSerializationProvider serializationProvider, Class<K> keyType, Class<V> valueType)
      throws org.ehcache.spi.serialization.UnsupportedTypeException {
    return new StoreConfigurationImpl<>(
      CacheConfigurationBuilder.newCacheConfigurationBuilder(keyType, valueType,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated(targetResource, 8, MemoryUnit.MB)))
        .build(),
      StoreEventSourceConfiguration.DEFAULT_DISPATCHER_CONCURRENCY,
      serializationProvider.createKeySerializer(keyType, getClass().getClassLoader()),
      serializationProvider.createValueSerializer(valueType, getClass().getClassLoader()));
  }

  private ClusteredCacheIdentifier getClusteredCacheIdentifier(
      DefaultClusteringService service, String cacheAlias)
      throws CachePersistenceException {

    ClusteredCacheIdentifier clusteredCacheIdentifier = (ClusteredCacheIdentifier) service.getPersistenceSpaceIdentifier(cacheAlias, null);
    if (clusteredCacheIdentifier != null) {
      return clusteredCacheIdentifier;
    }
    throw new AssertionError("ClusteredCacheIdentifier not available for configuration");
  }

  @Test
  public void testGetServerStoreProxyReturnsEventualStore() throws Exception {
    String entityIdentifier = "my-application";
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(
            URI.create(CLUSTER_URI_BASE + entityIdentifier),
            true, new ServerSideConfiguration(Collections.<String, Pool>emptyMap()));
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    ClusteringService.ClusteredCacheIdentifier cacheIdentifier = (ClusteredCacheIdentifier) service.getPersistenceSpaceIdentifier("my-cache", null);

    ResourcePools resourcePools = mock(ResourcePools.class);
    @SuppressWarnings("unchecked")
    Store.Configuration<String, Object> storeConfig = mock(Store.Configuration.class);
    when(storeConfig.getResourcePools()).thenReturn(resourcePools);
    when(resourcePools.getPoolForResource(eq(DEDICATED))).thenReturn(new DedicatedClusteredResourcePoolImpl("serverResource1", 1L, MemoryUnit.MB));
    when(storeConfig.getKeyType()).thenReturn(String.class);
    when(storeConfig.getValueType()).thenReturn(Object.class);

    ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(cacheIdentifier, storeConfig, Consistency.EVENTUAL, mock(ServerCallback.class));
    assertThat(serverStoreProxy, instanceOf(EventualServerStoreProxy.class));
  }

  @Test
  public void testGetServerStoreProxyReturnsStrongStore() throws Exception {
    String entityIdentifier = "my-application";
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(
            URI.create(CLUSTER_URI_BASE + entityIdentifier),
            true, new ServerSideConfiguration(Collections.<String, Pool>emptyMap()));
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    ClusteringService.ClusteredCacheIdentifier cacheIdentifier = (ClusteredCacheIdentifier) service.getPersistenceSpaceIdentifier("my-cache", null);

    ResourcePools resourcePools = mock(ResourcePools.class);
    @SuppressWarnings("unchecked")
    Store.Configuration<String, Object> storeConfig = mock(Store.Configuration.class);
    when(storeConfig.getResourcePools()).thenReturn(resourcePools);
    when(resourcePools.getPoolForResource(eq(DEDICATED))).thenReturn(new DedicatedClusteredResourcePoolImpl("serverResource1", 1L, MemoryUnit.MB));
    when(storeConfig.getKeyType()).thenReturn(String.class);
    when(storeConfig.getValueType()).thenReturn(Object.class);

    ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(cacheIdentifier, storeConfig, Consistency.STRONG, mock(ServerCallback.class));
    assertThat(serverStoreProxy, instanceOf(StrongServerStoreProxy.class));
  }

  @Test
  public void testGetServerStoreProxyFailureClearsEntityListeners() throws Exception {
    // Initial setup begin
    String entityIdentifier = "my-application";
    ClusteringServiceConfiguration configuration =
      new ClusteringServiceConfiguration(
        URI.create(CLUSTER_URI_BASE + entityIdentifier),
        true, new ServerSideConfiguration(Collections.<String, Pool>emptyMap()));
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    ClusteringService.ClusteredCacheIdentifier cacheIdentifier = (ClusteredCacheIdentifier) service.getPersistenceSpaceIdentifier("my-cache", null);

    ResourcePools resourcePools = mock(ResourcePools.class);
    @SuppressWarnings("unchecked")
    Store.Configuration<String, Object> storeConfig = mock(Store.Configuration.class);
    when(storeConfig.getResourcePools()).thenReturn(resourcePools);
    when(resourcePools.getPoolForResource(eq(DEDICATED))).thenReturn(new DedicatedClusteredResourcePoolImpl("serverResource1", 1L, MemoryUnit.MB));
    when(storeConfig.getKeyType()).thenReturn(String.class);
    when(storeConfig.getValueType()).thenReturn(Object.class);

    service.getServerStoreProxy(cacheIdentifier, storeConfig, Consistency.STRONG, mock(ServerCallback.class));  // Creates the store
    service.stop();
    // Initial setup end

    service.start(null);
    when(resourcePools.getPoolForResource(eq(DEDICATED))).thenReturn(new DedicatedClusteredResourcePoolImpl("serverResource1", 2L, MemoryUnit.MB));
    try {
      service.getServerStoreProxy(cacheIdentifier, storeConfig, Consistency.STRONG, mock(ServerCallback.class));
      fail("Server store proxy creation should have failed");
    } catch (CachePersistenceException cpe) {
      assertThat(cpe.getMessage(), containsString("Unable to create cluster tier proxy"));
      assertThat(getRootCause(cpe), instanceOf(InvalidServerStoreConfigurationException.class));
    }
  }

  @Test
  public void testGetServerStoreProxyFailureDoesNotClearOtherStoreEntityListeners() throws Exception {
    // Initial setup begin
    String entityIdentifier = "my-application";
    ClusteringServiceConfiguration configuration =
      new ClusteringServiceConfiguration(
        URI.create(CLUSTER_URI_BASE + entityIdentifier),
        true, new ServerSideConfiguration(Collections.<String, Pool>emptyMap()));
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    ClusteringService.ClusteredCacheIdentifier cacheIdentifier = (ClusteredCacheIdentifier) service.getPersistenceSpaceIdentifier("my-cache", null);

    ResourcePools resourcePools = mock(ResourcePools.class);
    @SuppressWarnings("unchecked")
    Store.Configuration<String, Object> storeConfig = mock(Store.Configuration.class);
    when(storeConfig.getResourcePools()).thenReturn(resourcePools);
    when(resourcePools.getPoolForResource(eq(DEDICATED))).thenReturn(new DedicatedClusteredResourcePoolImpl("serverResource1", 1L, MemoryUnit.MB));
    when(storeConfig.getKeyType()).thenReturn(String.class);
    when(storeConfig.getValueType()).thenReturn(Object.class);

    service.getServerStoreProxy(cacheIdentifier, storeConfig, Consistency.STRONG, mock(ServerCallback.class));  // Creates the store
    service.stop();
    // Initial setup end

    service.start(null);
    ClusteringService.ClusteredCacheIdentifier otherCacheIdentifier = (ClusteredCacheIdentifier) service.getPersistenceSpaceIdentifier("my-other-cache", null);
    service.getServerStoreProxy(otherCacheIdentifier, storeConfig, Consistency.STRONG, mock(ServerCallback.class));  // Creates one more store

    when(resourcePools.getPoolForResource(eq(DEDICATED))).thenReturn(new DedicatedClusteredResourcePoolImpl("serverResource1", 2L, MemoryUnit.MB));
    try {
      service.getServerStoreProxy(cacheIdentifier, storeConfig, Consistency.STRONG, mock(ServerCallback.class));
      fail("Server store proxy creation should have failed");
    } catch (CachePersistenceException cpe) {
      assertThat(cpe.getMessage(), containsString("Unable to create cluster tier proxy"));
      assertThat(getRootCause(cpe), instanceOf(InvalidServerStoreConfigurationException.class));
    }
  }

  @Test
  public void testGetStateRepositoryWithinTwiceWithSameName() throws Exception {
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(URI.create(CLUSTER_URI_BASE), true, new ServerSideConfiguration(Collections.<String, Pool>emptyMap()));
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    PersistableResourceService.PersistenceSpaceIdentifier<?> cacheIdentifier = service.getPersistenceSpaceIdentifier("myCache", null);
    StateRepository repository1 = service.getStateRepositoryWithin(cacheIdentifier, "myRepo");
    StateRepository repository2 = service.getStateRepositoryWithin(cacheIdentifier, "myRepo");
    assertThat(repository1, sameInstance(repository2));
  }

  @Test
  public void testGetStateRepositoryWithinTwiceWithSameNameDifferentPersistenceSpaceIdentifier() throws Exception {
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(URI.create(CLUSTER_URI_BASE), true, new ServerSideConfiguration(Collections.<String, Pool>emptyMap()));
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    PersistableResourceService.PersistenceSpaceIdentifier<?> cacheIdentifier1 = service.getPersistenceSpaceIdentifier("myCache1", null);
    PersistableResourceService.PersistenceSpaceIdentifier<?> cacheIdentifier2 = service.getPersistenceSpaceIdentifier("myCache2", null);
    StateRepository repository1 = service.getStateRepositoryWithin(cacheIdentifier1, "myRepo");
    StateRepository repository2 = service.getStateRepositoryWithin(cacheIdentifier2, "myRepo");
    assertThat(repository1, not(sameInstance(repository2)));
  }

  @Test
  public void testGetStateRepositoryWithinWithNonExistentPersistenceSpaceIdentifier() throws Exception {
    expectedException.expect(CachePersistenceException.class);
    expectedException.expectMessage("Clustered space not found for identifier");
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(URI.create(CLUSTER_URI_BASE), true, new ServerSideConfiguration(Collections.<String, Pool>emptyMap()));
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    ClusteredCacheIdentifier cacheIdentifier = mock(ClusteredCacheIdentifier.class);
    doReturn("foo").when(cacheIdentifier).getId();
    service.getStateRepositoryWithin(cacheIdentifier, "myRepo");
  }

  @Test
  public void testReleaseNonExistentPersistenceSpaceIdentifierTwice() throws Exception {
    expectedException.expect(CachePersistenceException.class);
    expectedException.expectMessage("Unknown identifier");
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(URI.create(CLUSTER_URI_BASE), true, new ServerSideConfiguration(Collections.<String, Pool>emptyMap()));
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    ClusteredCacheIdentifier cacheIdentifier = mock(ClusteredCacheIdentifier.class);
    doReturn("foo").when(cacheIdentifier).getId();
    service.releasePersistenceSpaceIdentifier(cacheIdentifier);
  }

  @Test
  public void testReleasePersistenceSpaceIdentifierTwice() throws Exception {
    expectedException.expect(CachePersistenceException.class);
    expectedException.expectMessage("Unknown identifier");
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(URI.create(CLUSTER_URI_BASE), true, new ServerSideConfiguration(Collections.<String, Pool>emptyMap()));
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    PersistableResourceService.PersistenceSpaceIdentifier<?> cacheIdentifier = service.getPersistenceSpaceIdentifier("myCache", null);
    try {
      service.releasePersistenceSpaceIdentifier(cacheIdentifier);
    } catch (CachePersistenceException e) {
      fail("First invocation of releasePersistenceSpaceIdentifier should not have failed");
    }
    service.releasePersistenceSpaceIdentifier(cacheIdentifier);
  }

  @Test
  public void releaseMaintenanceHoldsWhenConnectionClosedDuringDestruction() {
    ClusteringServiceConfiguration configuration =
      new ClusteringServiceConfiguration(URI.create(CLUSTER_URI_BASE), true, new ServerSideConfiguration(Collections.<String, Pool>emptyMap()));
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    assertThat(service.isConnected(), is(false));
    service.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);
    assertThat(service.isConnected(), is(true));

    ConnectionState connectionState = service.getConnectionState();
    ClusterTierManagerClientEntityFactory clusterTierManagerClientEntityFactory = connectionState.getEntityFactory();
    assertEquals(clusterTierManagerClientEntityFactory.getMaintenanceHolds().size(), 1);
    connectionState.destroyState(false);
    assertEquals(clusterTierManagerClientEntityFactory.getMaintenanceHolds().size(), 0);
    service.stop();
  }

  private static Throwable getRootCause(Throwable t) {
    if (t.getCause() == null || t.getCause() == t) {
      return t;
    }
    return getRootCause(t.getCause());
  }

}
