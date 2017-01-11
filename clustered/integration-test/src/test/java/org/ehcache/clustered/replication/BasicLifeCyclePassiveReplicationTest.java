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

package org.ehcache.clustered.replication;

import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock;
import org.ehcache.clustered.client.internal.service.ClusteredTierCreationException;
import org.ehcache.clustered.client.internal.service.ClusteredTierDestructionException;
import org.ehcache.clustered.client.internal.service.ClusteredTierManagerConfigurationException;
import org.ehcache.clustered.client.internal.service.ClusteredTierManagerValidationException;
import org.ehcache.clustered.client.internal.service.ClusteringServiceFactory;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreManagerException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.serialization.CompactJavaSerializer;
import org.ehcache.spi.service.MaintainableService;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collections;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BasicLifeCyclePassiveReplicationTest {

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">16</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  @ClassRule
  public static Cluster CLUSTER =
      new BasicExternalCluster(new File("build/cluster"), 2, Collections.<File>emptyList(), "", RESOURCE_CONFIG, "");

  @Before
  public void startServers() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().terminateActive();
  }

  @Test
  public void testCreateCacheReplication() throws Exception {

    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI())
            .autoCreate()
            .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    EhcacheClientEntity clientEntity = getEntity(service);

    clientEntity.createCache("testCache", getServerStoreConfiguration("primary-server-resource"));

    CLUSTER.getClusterControl().terminateActive();

    try {
      clientEntity.createCache("testCache", getServerStoreConfiguration("primary-server-resource"));
      fail("ClusteredTierCreationException Expected.");
    } catch (ClusteredTierCreationException e) {
      assertThat(e.getCause(), instanceOf(InvalidStoreException.class));
      assertThat(e.getCause().getMessage(), is("Clustered tier 'testCache' already exists"));
    }

    service.stop();
    cleanUpCluster(service);
  }

  @Test
  public void testDestroyCacheReplication() throws Exception {

    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI())
            .autoCreate()
            .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    EhcacheClientEntity clientEntity = getEntity(service);

    clientEntity.createCache("testCache", getServerStoreConfiguration("primary-server-resource"));

    clientEntity.releaseCache("testCache");
    clientEntity.destroyCache("testCache");

    CLUSTER.getClusterControl().terminateActive();

    try {
      clientEntity.destroyCache("testCache");
      fail("ClusteredTierReleaseException Expected.");
    } catch (ClusteredTierDestructionException e) {
      assertThat(e.getCause(), instanceOf(InvalidStoreException.class));
      assertThat(e.getCause().getMessage(), is("Clustered tier 'testCache' does not exist"));
    }

    service.stop();
    cleanUpCluster(service);
  }

  @Test
  public void testConfigureReplication() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI())
            .autoCreate()
            .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    EhcacheClientEntity clientEntity = getEntity(service);

    CLUSTER.getClusterControl().terminateActive();

    try {
      clientEntity.configure(configuration.getServerConfiguration());
      fail("ClusteredTierManagerConfigurationException Expected.");
    } catch (ClusteredTierManagerConfigurationException e) {
      assertThat(e.getCause(), instanceOf(InvalidStoreManagerException.class));
      assertThat(e.getCause().getMessage(), is("Clustered Tier Manager already configured"));
    }

    service.stop();
    cleanUpCluster(service);
  }

  @Test
  public void testValidateReplication() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI())
            .autoCreate()
            .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    EhcacheClientEntity clientEntity = getEntity(service);

    CLUSTER.getClusterControl().terminateActive();

    try {
      clientEntity.validate(configuration.getServerConfiguration());
      fail("LifecycleException Expected.");
    } catch (ClusteredTierManagerValidationException e) {
      assertThat(e.getCause(), instanceOf(LifecycleException.class));
      assertThat(e.getCause().getMessage(), containsString("is already being tracked with Client Id"));
    }

    service.stop();
    cleanUpCluster(service);
  }

  @Test
  public void testDestroyServerStoreIsNotReplicatedIfFailsOnActive() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI())
            .autoCreate()
            .build();

    ClusteringService service1 = new ClusteringServiceFactory().create(configuration);

    ClusteringService service2 = new ClusteringServiceFactory().create(configuration);

    service1.start(null);
    service2.start(null);

    EhcacheClientEntity clientEntity1 = getEntity(service1);
    EhcacheClientEntity clientEntity2 = getEntity(service2);

    clientEntity1.createCache("testCache", getServerStoreConfiguration("primary-server-resource"));
    clientEntity2.validateCache("testCache", getServerStoreConfiguration("primary-server-resource"));

    clientEntity1.releaseCache("testCache");
    try {
      clientEntity1.destroyCache("testCache");
      fail("ClusteredTierDestructionException Expected");
    } catch (ClusteredTierDestructionException e) {
      //nothing to do
    }

    CLUSTER.getClusterControl().terminateActive();

    clientEntity2.releaseCache("testCache");
    clientEntity2.destroyCache("testCache");

    service1.stop();
    service2.stop();
    cleanUpCluster(service1);
  }

  @Test
  public void testDestroyCacheManager() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> configBuilder = newCacheManagerBuilder().with(cluster(CLUSTER.getConnectionURI().resolve("/destroy-CM"))
      .autoCreate().defaultServerResource("primary-server-resource"));
    PersistentCacheManager cacheManager1 = configBuilder.build(true);
    PersistentCacheManager cacheManager2 = configBuilder.build(true);

    cacheManager2.close();

    try {
      cacheManager2.destroy();
      fail("Exception expected");
    } catch (Exception e) {
      e.printStackTrace();
    }

    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();

    cacheManager1.createCache("test", newCacheConfigurationBuilder(Long.class, String.class, heap(10).with(clusteredDedicated(10, MB))));
  }

  @Test
  public void testDestroyLockEntity() throws Exception {
    VoltronReadWriteLock lock1 = new VoltronReadWriteLock(CLUSTER.newConnection(), "my-lock");
    VoltronReadWriteLock.Hold hold1 = lock1.tryReadLock();

    VoltronReadWriteLock lock2 = new VoltronReadWriteLock(CLUSTER.newConnection(), "my-lock");
    assertThat(lock2.tryWriteLock(), nullValue());

    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();

    hold1.unlock();
  }

  private static EhcacheClientEntity getEntity(ClusteringService clusteringService) throws NoSuchFieldException, IllegalAccessException {
    Field entity = clusteringService.getClass().getDeclaredField("entity");
    entity.setAccessible(true);
    return (EhcacheClientEntity)entity.get(clusteringService);
  }

  private void cleanUpCluster(ClusteringService service) throws CachePersistenceException {
    service.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);
    service.destroyAll();
    service.stop();
  }

  private static ServerStoreConfiguration getServerStoreConfiguration(String resourceName) {
    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(resourceName, 4, MB);
    return new ServerStoreConfiguration(resourcePool.getPoolAllocation(),
        String.class.getName(), String.class.getName(), null, null, CompactJavaSerializer.class.getName(), CompactJavaSerializer.class
        .getName(), Consistency.STRONG);
  }


}
