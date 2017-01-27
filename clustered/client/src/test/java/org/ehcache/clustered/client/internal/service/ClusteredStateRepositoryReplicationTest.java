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
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.EhcacheClientEntityService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.client.internal.store.ClusteredTierClientEntityService;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.SimpleClusteredTierClientEntity;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.EhcacheServerEntityService;
import org.ehcache.clustered.server.store.ClusteredTierServerEntityService;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.internal.store.StoreConfigurationImpl;
import org.ehcache.spi.persistence.StateHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.passthrough.PassthroughClusterControl;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.lang.reflect.Field;
import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.internal.UnitTestConnectionService.getOffheapResourcesType;
import static org.ehcache.config.Eviction.noAdvice;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.expiry.Expirations.noExpiration;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ClusteredStateRepositoryReplicationTest {

  private PassthroughClusterControl clusterControl;
  private static String STRIPENAME = "stripe";
  private static String STRIPE_URI = "passthrough://" + STRIPENAME;

  @Before
  public void setUp() throws Exception {
    this.clusterControl = PassthroughTestHelpers.createActivePassive(STRIPENAME,
      server -> {
        server.registerServerEntityService(new EhcacheServerEntityService());
        server.registerClientEntityService(new EhcacheClientEntityService());
        server.registerServerEntityService(new ClusteredTierServerEntityService());
        server.registerClientEntityService(new ClusteredTierClientEntityService());
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
  public void testClusteredStateRepositoryReplication() throws Exception {
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

    ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(spaceIdentifier, new StoreConfigurationImpl<>(config, 1, null, null), Consistency.STRONG);

    SimpleClusteredTierClientEntity clientEntity = getEntity(serverStoreProxy);

    ClusteredStateRepository stateRepository = new ClusteredStateRepository(spaceIdentifier, "test", clientEntity);

    StateHolder<String, String> testHolder = stateRepository.getPersistentStateHolder("testHolder", String.class, String.class);
    testHolder.putIfAbsent("One", "One");
    testHolder.putIfAbsent("Two", "Two");

    clusterControl.terminateActive();
    clusterControl.waitForActive();

    assertThat(testHolder.get("One"), is("One"));
    assertThat(testHolder.get("Two"), is("Two"));

    service.stop();
  }

  private static SimpleClusteredTierClientEntity getEntity(ServerStoreProxy clusteringService) throws NoSuchFieldException, IllegalAccessException {
    Field entity = clusteringService.getClass().getDeclaredField("entity");
    entity.setAccessible(true);
    return (SimpleClusteredTierClientEntity)entity.get(clusteringService);
  }

}
