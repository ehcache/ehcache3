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
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntityService;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.ServerCallback;
import org.ehcache.clustered.client.internal.store.SimpleClusterTierClientEntity;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.ClusterTierManagerServerEntityService;
import org.ehcache.clustered.server.store.ClusterTierServerEntityService;
import org.ehcache.impl.config.BaseCacheConfiguration;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.spi.persistence.StateHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.passthrough.PassthroughClusterControl;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.internal.UnitTestConnectionService.getOffheapResourcesType;
import static org.ehcache.config.Eviction.noAdvice;
import static org.ehcache.config.builders.ExpiryPolicyBuilder.noExpiration;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class ClusterStateRepositoryReplicationTest {

  private PassthroughClusterControl clusterControl;
  private static String STRIPENAME = "stripe";
  private static String STRIPE_URI = "passthrough://" + STRIPENAME;

  @Before
  public void setUp() throws Exception {
    this.clusterControl = PassthroughTestHelpers.createActivePassive(STRIPENAME,
      server -> {
        server.registerServerEntityService(new ClusterTierManagerServerEntityService());
        server.registerClientEntityService(new ClusterTierManagerClientEntityService());
        server.registerServerEntityService(new ClusterTierServerEntityService());
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
  public void testClusteredStateRepositoryReplication() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(STRIPE_URI))
            .autoCreate(c -> c)
            .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);
    try {
      BaseCacheConfiguration<Long, String> config = new BaseCacheConfiguration<>(Long.class, String.class, noAdvice(), null, noExpiration(),
        newResourcePoolsBuilder().with(clusteredDedicated("test", 2, org.ehcache.config.units.MemoryUnit.MB)).build());
      ClusteringService.ClusteredCacheIdentifier spaceIdentifier = (ClusteringService.ClusteredCacheIdentifier) service.getPersistenceSpaceIdentifier("test",
        config);

      ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(spaceIdentifier, new StoreConfigurationImpl<>(config, 1, null, null), Consistency.STRONG, mock(ServerCallback.class));

      SimpleClusterTierClientEntity clientEntity = getEntity(serverStoreProxy);

      ClusterStateRepository stateRepository = new ClusterStateRepository(spaceIdentifier, "test", clientEntity);

      StateHolder<String, String> testHolder = stateRepository.getPersistentStateHolder("testHolder", String.class, String.class, c -> true, null);
      testHolder.putIfAbsent("One", "One");
      testHolder.putIfAbsent("Two", "Two");

      clusterControl.terminateActive();
      clusterControl.waitForActive();

      assertThat(testHolder.get("One"), is("One"));
      assertThat(testHolder.get("Two"), is("Two"));
    } finally {
      service.stop();
    }
  }

  @Test
  public void testClusteredStateRepositoryReplicationWithSerializableKV() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(STRIPE_URI))
            .autoCreate(c -> c)
            .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);
    try {
      BaseCacheConfiguration<Long, String> config = new BaseCacheConfiguration<>(Long.class, String.class, noAdvice(), null, noExpiration(),
        newResourcePoolsBuilder().with(clusteredDedicated("test", 2, org.ehcache.config.units.MemoryUnit.MB)).build());
      ClusteringService.ClusteredCacheIdentifier spaceIdentifier = (ClusteringService.ClusteredCacheIdentifier) service.getPersistenceSpaceIdentifier("test",
        config);

      ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(spaceIdentifier, new StoreConfigurationImpl<>(config, 1, null, null), Consistency.STRONG, mock(ServerCallback.class));

      SimpleClusterTierClientEntity clientEntity = getEntity(serverStoreProxy);

      ClusterStateRepository stateRepository = new ClusterStateRepository(new ClusteringService.ClusteredCacheIdentifier() {
        @Override
        public String getId() {
          return "testStateRepo";
        }

        @Override
        public Class<ClusteringService> getServiceType() {
          return ClusteringService.class;
        }
      }, "test", clientEntity);

      StateHolder<TestVal, TestVal> testMap = stateRepository.getPersistentStateHolder("testMap", TestVal.class, TestVal.class, c -> true, null);
      testMap.putIfAbsent(new TestVal("One"), new TestVal("One"));
      testMap.putIfAbsent(new TestVal("Two"), new TestVal("Two"));

      clusterControl.terminateActive();
      clusterControl.waitForActive();

      assertThat(testMap.get(new TestVal("One")), is(new TestVal("One")));
      assertThat(testMap.get(new TestVal("Two")), is(new TestVal("Two")));

      assertThat(testMap.entrySet(), hasSize(2));
    } finally {
      service.stop();
    }
  }

  private static SimpleClusterTierClientEntity getEntity(ServerStoreProxy clusteringService) throws NoSuchFieldException, IllegalAccessException {
    Field entity = clusteringService.getClass().getDeclaredField("entity");
    entity.setAccessible(true);
    return (SimpleClusterTierClientEntity)entity.get(clusteringService);
  }

  private static class TestVal implements Serializable {

    private static final long serialVersionUID = 1L;

    final String val;


    private TestVal(String val) {
      this.val = val;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TestVal testVal = (TestVal) o;

      return val != null ? val.equals(testVal.val) : testVal.val == null;
    }

    @Override
    public int hashCode() {
      return val != null ? val.hashCode() : 0;
    }
  }

}
