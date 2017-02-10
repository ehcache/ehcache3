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
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.client.internal.EhcacheClientEntityService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.EhcacheServerEntityService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.passthrough.PassthroughClusterControl;
import org.terracotta.passthrough.PassthroughServer;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.concurrent.ConcurrentMap;

import static org.ehcache.clustered.client.internal.UnitTestConnectionService.getOffheapResourcesType;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ClusteredStateRepositoryReplicationTest {

  private PassthroughClusterControl clusterControl;
  private static String STRIPENAME = "stripe";
  private static String STRIPE_URI = "passthrough://" + STRIPENAME;

  @Before
  public void setUp() throws Exception {
    this.clusterControl = PassthroughTestHelpers.createActivePassive(STRIPENAME,
        new PassthroughTestHelpers.ServerInitializer() {
          @Override
          public void registerServicesForServer(PassthroughServer server) {
            server.registerServerEntityService(new EhcacheServerEntityService());
            server.registerClientEntityService(new EhcacheClientEntityService());
            server.registerServerEntityService(new VoltronReadWriteLockServerEntityService());
            server.registerClientEntityService(new VoltronReadWriteLockEntityClientService());
            server.registerExtendedConfiguration(new OffHeapResourcesProvider(getOffheapResourcesType("test", 32, MemoryUnit.MB)));

            UnitTestConnectionService.addServerToStripe(STRIPENAME, server);
          }
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

    EhcacheClientEntity clientEntity = getEntity(service);

    ClusteredStateRepository stateRepository = new ClusteredStateRepository(new ClusteringService.ClusteredCacheIdentifier() {
      @Override
      public String getId() {
        return "testStateRepo";
      }

      @Override
      public Class<ClusteringService> getServiceType() {
        return ClusteringService.class;
      }
    }, "test", clientEntity);

    ConcurrentMap<String, String> testMap = stateRepository.getPersistentConcurrentMap("testMap", String.class, String.class);
    testMap.putIfAbsent("One", "One");
    testMap.putIfAbsent("Two", "Two");

    clusterControl.terminateActive();
    clusterControl.waitForActive();

    assertThat(testMap.get("One"), is("One"));
    assertThat(testMap.get("Two"), is("Two"));

    service.stop();
  }

  @Test
  public void testClusteredStateRepositoryReplicationWithSerializableKV() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(STRIPE_URI))
            .autoCreate()
            .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    EhcacheClientEntity clientEntity = getEntity(service);

    ClusteredStateRepository stateRepository = new ClusteredStateRepository(new ClusteringService.ClusteredCacheIdentifier() {
      @Override
      public String getId() {
        return "testStateRepo";
      }

      @Override
      public Class<ClusteringService> getServiceType() {
        return ClusteringService.class;
      }
    }, "test", clientEntity);

    ConcurrentMap<TestVal, TestVal> testMap = stateRepository.getPersistentConcurrentMap("testMap", TestVal.class, TestVal.class);
    testMap.putIfAbsent(new TestVal("One"), new TestVal("One"));
    testMap.putIfAbsent(new TestVal("Two"), new TestVal("Two"));

    clusterControl.terminateActive();
    clusterControl.waitForActive();

    assertThat(testMap.get(new TestVal("One")), is(new TestVal("One")));
    assertThat(testMap.get(new TestVal("Two")), is(new TestVal("Two")));

    assertThat(testMap.entrySet(), hasSize(2));

    service.stop();
  }

  private static EhcacheClientEntity getEntity(ClusteringService clusteringService) throws NoSuchFieldException, IllegalAccessException {
    Field entity = clusteringService.getClass().getDeclaredField("entity");
    entity.setAccessible(true);
    return (EhcacheClientEntity)entity.get(clusteringService);
  }

  private static class TestVal implements Serializable {
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
