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
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.impl.internal.store.shared.StateHolderIdGenerator;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.spi.persistence.PersistableIdentityService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ClusteredStateHolderIdGeneratorTest {
  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/");
  private ServiceLocator serviceLocator;
  private StateHolderIdGenerator<String> sharedPersistence;
  private PersistableIdentityService.PersistenceSpaceIdentifier<?> spaceIdentifier;
  private DefaultClusteringService clusterService;

  @Before
  public void definePassthroughServer() throws CachePersistenceException {
    UnitTestConnectionService.add(CLUSTER_URI,
      new UnitTestConnectionService.PassthroughServerBuilder()
        .resource("defaultResource", 128, MemoryUnit.MB)
        .build());

    clusterService = new DefaultClusteringService(cluster(CLUSTER_URI).autoCreate(c -> c).build());
    ServiceLocator.DependencySet dependencySet = dependencySet();
    dependencySet.with(clusterService);
    serviceLocator = dependencySet.build();
    serviceLocator.startAllServices();

    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated("defaultResource", 4, MemoryUnit.MB);
    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(resourcePool.getPoolAllocation(),
      Long.class.getName(), String.class.getName(), LongSerializer.class.getName(), StringSerializer.class.getName(), null, false);
    ClusterTierClientEntity clientEntity = clusterService.getConnectionState().createClusterTierClientEntity("CacheManagerSharedResources", serverStoreConfiguration, false);
    assertThat(clientEntity, notNullValue());

    spaceIdentifier = clusterService.getRootSpaceIdentifier(true);
    sharedPersistence = new StateHolderIdGenerator<>(clusterService.getStateRepositoryWithin(spaceIdentifier, "persistent-partition-ids"), String.class);
  }

  @After
  public void removePassthroughServer() {
    try {
      UnitTestConnectionService.remove(CLUSTER_URI);
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), is("Connection already closed"));
    }
  }

  @Test
  public void testStateHolderIdGeneration() throws Exception {

    Set<String> namesToMap = getNames();
    map(namesToMap);
    clusterService.releasePersistenceSpaceIdentifier(spaceIdentifier);

    spaceIdentifier = clusterService.getRootSpaceIdentifier(true);
    namesToMap.addAll(getNames());
    map(namesToMap);
    clusterService.releasePersistenceSpaceIdentifier(spaceIdentifier);

    spaceIdentifier = clusterService.getRootSpaceIdentifier(true);
    namesToMap.addAll(getNames());
    map(namesToMap);
    clusterService.releasePersistenceSpaceIdentifier(spaceIdentifier);
    serviceLocator.stopAllServices();
  }

  private void map(Set<String> names) throws InterruptedException {
    Map<Integer, String> t1Results = new HashMap<>();
    Map<Integer, String> t2Results = new HashMap<>();
    Map<Integer, String> t3Results = new HashMap<>();
    Thread t1 = new Thread(() -> names.forEach(name -> t1Results.put(sharedPersistence.map(name), name)));
    Thread t2 = new Thread(() -> names.forEach(name -> t2Results.put(sharedPersistence.map(name), name)));
    Thread t3 = new Thread(() -> names.forEach(name -> t3Results.put(sharedPersistence.map(name), name)));
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();
    assertThat(t1Results, is(t2Results));
    assertThat(t1Results, is(t3Results));
    assertThat(t2Results, is(t3Results));
    assertThat(t1Results.size(), is(names.size()));
    assertThat(t2Results.size(), is(names.size()));
    assertThat(t3Results.size(), is(names.size()));
  }

  private Set<String> getNames() {
    Set<String> names = new HashSet<>();
    for (int i = 0; i < 200; i++) {
      names.add(UUID.randomUUID().toString());
    }
    return names;
  }
}
