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

package org.ehcache.clustered.client.replication;

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.client.internal.service.ClusteredStateHolder;
import org.ehcache.clustered.client.internal.service.ClusteringServiceFactory;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntityService;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.ObservableEhcacheServerEntityService;
import org.ehcache.clustered.server.store.ObservableClusterTierServerEntityService;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.internal.store.StoreConfigurationImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.passthrough.PassthroughClusterControl;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.internal.UnitTestConnectionService.getOffheapResourcesType;
import static org.ehcache.clustered.client.replication.ReplicationUtil.getEntity;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.ehcache.clustered.common.internal.store.Util.getElement;
import static org.ehcache.config.Eviction.noAdvice;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.expiry.Expirations.noExpiration;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ActivePassiveClientIdTest {

  private PassthroughClusterControl clusterControl;
  private static String STRIPENAME = "stripe";
  private static String STRIPE_URI = "passthrough://" + STRIPENAME;
  private ObservableEhcacheServerEntityService observableEhcacheServerEntityService;
  private ObservableClusterTierServerEntityService observableClusterTierServerEntityService;

  @Before
  public void setUp() throws Exception {
    this.observableEhcacheServerEntityService = new ObservableEhcacheServerEntityService();
    this.observableClusterTierServerEntityService = new ObservableClusterTierServerEntityService();
    this.clusterControl = PassthroughTestHelpers.createActivePassive(STRIPENAME,
        server -> {
          server.registerServerEntityService(observableEhcacheServerEntityService);
          server.registerClientEntityService(new ClusterTierManagerClientEntityService());
          server.registerServerEntityService(observableClusterTierServerEntityService);
          server.registerClientEntityService(new ClusterTierClientEntityService());
          server.registerServerEntityService(new VoltronReadWriteLockServerEntityService());
          server.registerClientEntityService(new VoltronReadWriteLockEntityClientService());
          server.registerExtendedConfiguration(new OffHeapResourcesProvider(getOffheapResourcesType("test", 32, MemoryUnit.MB)));

          UnitTestConnectionService.addServerToStripe(STRIPENAME, server);
        }
    );

    clusterControl.waitForActive();
    clusterControl.waitForRunningPassivesInStandby();
  }

  @After
  public void tearDown() throws Exception {
    UnitTestConnectionService.removeStripe(STRIPENAME);
    clusterControl.tearDown();
  }

  @Test
  public void testClientIdGetsTrackedAtPassive() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(STRIPE_URI))
            .autoCreate()
            .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    BaseCacheConfiguration<Long, String> config = new BaseCacheConfiguration<>(Long.class, String.class, noAdvice(), null, noExpiration(),
      newResourcePoolsBuilder().with(clusteredDedicated("test", 2, org.ehcache.config.units.MemoryUnit.MB)).build());
    ClusteringService.ClusteredCacheIdentifier spaceIdentifier = (ClusteringService.ClusteredCacheIdentifier) service.getPersistenceSpaceIdentifier("test",
      config);

    ServerStoreProxy storeProxy = service.getServerStoreProxy(spaceIdentifier, new StoreConfigurationImpl<>(config, 1, null, null), Consistency.STRONG);

    ObservableClusterTierServerEntityService.ObservableClusterTierPassiveEntity ehcachePassiveEntity = observableClusterTierServerEntityService.getServedPassiveEntities().get(0);

    assertThat(ehcachePassiveEntity.getMessageTrackerMap("test").size(), is(0));

    storeProxy.getAndAppend(42L, createPayload(42L));

    assertThat(ehcachePassiveEntity.getMessageTrackerMap("test").size(), is(1));

    service.stop();

    CompletableFuture<Boolean> completableFuture = CompletableFuture.supplyAsync(() -> {
      while (true) {
        try {
          if (ehcachePassiveEntity.getMessageTrackerMap("test").size() == 0) {
            return true;
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    assertThat(completableFuture.get(2, TimeUnit.SECONDS), is(true));

  }

  @Test
  public void testNotTrackedMessages() throws Exception {

    ClusteringServiceConfiguration configuration =
            ClusteringServiceConfigurationBuilder.cluster(URI.create(STRIPE_URI))
                    .autoCreate()
                    .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    BaseCacheConfiguration<Long, String> config = new BaseCacheConfiguration<>(Long.class, String.class, noAdvice(), null, noExpiration(),
            newResourcePoolsBuilder().with(clusteredDedicated("test", 2, org.ehcache.config.units.MemoryUnit.MB)).build());
    ClusteringService.ClusteredCacheIdentifier spaceIdentifier = (ClusteringService.ClusteredCacheIdentifier) service.getPersistenceSpaceIdentifier("test",
            config);

    ServerStoreProxy storeProxy = service.getServerStoreProxy(spaceIdentifier, new StoreConfigurationImpl<>(config, 1, null, null), Consistency.STRONG);

    ObservableClusterTierServerEntityService.ObservableClusterTierPassiveEntity ehcachePassiveEntity = observableClusterTierServerEntityService.getServedPassiveEntities().get(0);

    assertThat(ehcachePassiveEntity.getMessageTrackerMap("test").size(), is(0));

    List<Element> elements = new ArrayList<>();
    elements.add(getElement(createPayload(44L)));

    storeProxy.replaceAtHead(44L, getChain(elements), getChain(new ArrayList<Element>()));

    storeProxy.get(42L);

    assertThat(ehcachePassiveEntity.getMessageTrackerMap("test").size(), is(0));

    service.stop();

  }

}
