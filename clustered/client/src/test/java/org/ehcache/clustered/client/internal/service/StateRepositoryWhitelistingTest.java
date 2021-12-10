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
import org.ehcache.clustered.client.internal.store.SimpleClusterTierClientEntity;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.ClusterTierManagerServerEntityService;
import org.ehcache.clustered.server.store.ClusterTierServerEntityService;
import org.ehcache.core.config.BaseCacheConfiguration;
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
import java.util.Arrays;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.internal.UnitTestConnectionService.getOffheapResourcesType;
import static org.ehcache.config.Eviction.noAdvice;
import static org.ehcache.config.builders.ExpiryPolicyBuilder.noExpiration;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class StateRepositoryWhitelistingTest {

  private PassthroughClusterControl clusterControl;
  private static String STRIPENAME = "stripe";
  private static String STRIPE_URI = "passthrough://" + STRIPENAME;
  private ClusteringService service;
  ClusterStateRepository stateRepository;

  @Before
  public void setUp() throws Exception {
    this.clusterControl = PassthroughTestHelpers.createActiveOnly(STRIPENAME,
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

    ClusteringServiceConfiguration configuration =
      ClusteringServiceConfigurationBuilder.cluster(URI.create(STRIPE_URI))
        .autoCreate()
        .build();

    service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    BaseCacheConfiguration<Long, String> config = new BaseCacheConfiguration<>(Long.class, String.class, noAdvice(), null, noExpiration(),
      newResourcePoolsBuilder().with(clusteredDedicated("test", 2, org.ehcache.config.units.MemoryUnit.MB)).build());
    ClusteringService.ClusteredCacheIdentifier spaceIdentifier = (ClusteringService.ClusteredCacheIdentifier) service.getPersistenceSpaceIdentifier("test",
      config);

    ServerStoreProxy serverStoreProxy = service.getServerStoreProxy(spaceIdentifier,
      new StoreConfigurationImpl<>(config, 1, null, null),
      Consistency.STRONG,
      mock(ServerStoreProxy.ServerCallback.class));

    SimpleClusterTierClientEntity clientEntity = getEntity(serverStoreProxy);

    stateRepository = new ClusterStateRepository(new ClusteringService.ClusteredCacheIdentifier() {
      @Override
      public String getId() {
        return "testStateRepo";
      }

      @Override
      public Class<ClusteringService> getServiceType() {
        return ClusteringService.class;
      }
    }, "test", clientEntity);
  }

  @After
  public void tearDown() throws Exception {
    service.stop();
    UnitTestConnectionService.removeStripe(STRIPENAME);
    clusterControl.tearDown();
  }

  private static SimpleClusterTierClientEntity getEntity(ServerStoreProxy clusteringService) throws NoSuchFieldException, IllegalAccessException {
    Field entity = clusteringService.getClass().getDeclaredField("entity");
    entity.setAccessible(true);
    return (SimpleClusterTierClientEntity)entity.get(clusteringService);
  }

  @Test
  public void testWhiteListedClass() throws Exception {
    StateHolder<Child, Child> testMap = stateRepository.getPersistentStateHolder("testMap", Child.class, Child.class,
      Arrays.asList(Child.class, Parent.class)::contains, null);

    testMap.putIfAbsent(new Child(10, 20L), new Child(20, 30L));


    assertThat(testMap.get(new Child(10, 20L)), is(new Child(20, 30L)));

    assertThat(testMap.entrySet(), hasSize(1));
  }

  @Test
  public void testWhiteListedMissingClass() throws Exception {
    StateHolder<Child, Child> testMap = stateRepository.getPersistentStateHolder("testMap", Child.class, Child.class,
      Arrays.asList(Child.class)::contains, null);

    testMap.putIfAbsent(new Child(10, 20L), new Child(20, 30L));

    try {
      assertThat(testMap.entrySet(), hasSize(1));
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().equals("Could not load class"));
    }
  }

  @Test
  public void testWhitelistingForPrimitiveClass() throws Exception {
    // No whitelisting for primitive classes are required as we do not deserialize them at client side
    StateHolder<Integer, Integer> testMap = stateRepository.getPersistentStateHolder("testMap", Integer.class, Integer.class,
      Arrays.asList(Child.class)::contains, null);

    testMap.putIfAbsent(new Integer(10), new Integer(20));

    assertThat(testMap.get(new Integer(10)), is(new Integer(20)));
    assertThat(testMap.entrySet(), hasSize(1));
  }

  private static class Parent implements Serializable {

    private static final long serialVersionUID = 1L;

    final int val;

    private Parent(int val) {
      this.val = val;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Parent)) return false;

      Parent testVal = (Parent) o;

      return val == testVal.val;
    }

    @Override
    public int hashCode() {
      return val;
    }
  }

  private static class Child extends Parent implements Serializable {

    private static final long serialVersionUID = 1L;

    final long longValue;

    private Child(int val, long longValue) {
      super(val);
      this.longValue = longValue;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Child)) return false;

      Child testVal = (Child) o;

      return super.equals(testVal) && longValue == testVal.longValue;
    }

    @Override
    public int hashCode() {
      return super.hashCode() + 31 * Long.hashCode(longValue);
    }
  }
}
