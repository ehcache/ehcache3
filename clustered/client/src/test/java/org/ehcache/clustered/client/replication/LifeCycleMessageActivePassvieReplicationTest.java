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
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.client.internal.EhcacheClientEntityService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.client.internal.service.ClusteredTierCreationException;
import org.ehcache.clustered.client.internal.service.ClusteredTierDestructionException;
import org.ehcache.clustered.client.internal.service.ClusteredTierManagerConfigurationException;
import org.ehcache.clustered.client.internal.service.ClusteringServiceFactory;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreManagerException;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.EhcacheServerEntityService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.passthrough.PassthroughClusterControl;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.net.URI;

import static org.ehcache.clustered.client.internal.UnitTestConnectionService.getOffheapResourcesType;
import static org.ehcache.clustered.client.replication.ReplicationUtil.getEntity;
import static org.ehcache.clustered.client.replication.ReplicationUtil.getServerStoreConfiguration;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class LifeCycleMessageActivePassvieReplicationTest {

  private PassthroughClusterControl clusterControl;
  private static String STRIPENAME = "stripe";
  private static String STRIPE_URI = "passthrough://" + STRIPENAME;

  @Before
  public void setUp() throws Exception {
    this.clusterControl = PassthroughTestHelpers.createActivePassive(STRIPENAME,
        server -> {
            server.registerServerEntityService(new EhcacheServerEntityService());
            server.registerClientEntityService(new EhcacheClientEntityService());
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
  public void testConfigureReplication() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(STRIPE_URI))
            .autoCreate()
            .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    EhcacheClientEntity clientEntity = getEntity(service);

    clusterControl.terminateActive();
    clusterControl.waitForActive();

    try {
      clientEntity.configure(configuration.getServerConfiguration());
      fail("ClusteredTierManagerConfigurationException Expected.");
    } catch (ClusteredTierManagerConfigurationException e) {
      assertThat(e.getCause(), instanceOf(InvalidStoreManagerException.class));
      assertThat(e.getCause().getMessage(), is("Clustered Tier Manager already configured"));
    }

    service.stop();
  }

  @Test
  public void testCreateServerStoreReplication() throws Exception {

    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(STRIPE_URI))
            .autoCreate()
            .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    EhcacheClientEntity clientEntity = getEntity(service);

    clientEntity.createCache("testCache", getServerStoreConfiguration("test"));

    clusterControl.terminateActive();
    clusterControl.waitForActive();

    try {
      clientEntity.createCache("testCache", getServerStoreConfiguration("test"));
      fail("ClusteredTierCreationException Expected.");
    } catch (ClusteredTierCreationException e) {
      assertThat(e.getCause(), instanceOf(InvalidStoreException.class));
      assertThat(e.getCause().getMessage(), is("Clustered tier 'testCache' already exists"));
    }

    service.stop();

  }

  @Test
  public void testDestroyServerStoreReplication() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(STRIPE_URI))
            .autoCreate()
            .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    EhcacheClientEntity clientEntity = getEntity(service);

    clientEntity.createCache("testCache", getServerStoreConfiguration("test"));

    clientEntity.releaseCache("testCache");
    clientEntity.destroyCache("testCache");

    clusterControl.terminateActive();

    try {
      clientEntity.destroyCache("testCache");
      fail("ClusteredTierReleaseException Expected.");
    } catch (ClusteredTierDestructionException e) {
      assertThat(e.getCause(), instanceOf(InvalidStoreException.class));
      assertThat(e.getCause().getMessage(), is("Clustered tier 'testCache' does not exist"));
    }

    service.stop();

  }

  @Test
  public void testDestroyServerStoreIsNotReplicatedIfFailsOnActive() throws Exception {
    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(URI.create(STRIPE_URI))
            .autoCreate()
            .build();

    ClusteringService service1 = new ClusteringServiceFactory().create(configuration);

    ClusteringService service2 = new ClusteringServiceFactory().create(configuration);

    service1.start(null);
    service2.start(null);

    EhcacheClientEntity clientEntity1 = getEntity(service1);
    EhcacheClientEntity clientEntity2 = getEntity(service2);

    clientEntity1.createCache("testCache", getServerStoreConfiguration("test"));
    clientEntity2.validateCache("testCache", getServerStoreConfiguration("test"));

    clientEntity1.releaseCache("testCache");
    try {
      clientEntity1.destroyCache("testCache");
      fail("ClusteredTierDestructionException Expected");
    } catch (ClusteredTierDestructionException e) {
      //nothing to do
    }

    clusterControl.terminateActive();

    clientEntity2.releaseCache("testCache");
    clientEntity2.destroyCache("testCache");

    service1.stop();
    service2.stop();

  }

}
