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
import org.ehcache.clustered.client.internal.service.ClusteringServiceFactory;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntityService;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.ServerCallback;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.ObservableEhcacheServerEntityService;
import org.ehcache.clustered.server.store.ObservableClusterTierServerEntityService;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.client.message.tracker.OOOMessageHandler;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.passthrough.PassthroughClusterControl;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.net.URI;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.ehcache.clustered.ChainUtils.chainOf;
import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.internal.UnitTestConnectionService.getOffheapResourcesType;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.mockito.Mockito.mock;

public class ActivePassiveClientIdTest {

  private static final String STRIPENAME = "stripe";
  private static final String STRIPE_URI = "passthrough://" + STRIPENAME;
  private static final int KEY_ENDS_UP_IN_SEGMENT_11 = 11;


  private PassthroughClusterControl clusterControl;

  private ObservableEhcacheServerEntityService observableEhcacheServerEntityService;
  private ObservableClusterTierServerEntityService observableClusterTierServerEntityService;

  private ClusteringService service;
  private ServerStoreProxy storeProxy;

  private ObservableClusterTierServerEntityService.ObservableClusterTierActiveEntity activeEntity;
  private ObservableClusterTierServerEntityService.ObservableClusterTierPassiveEntity passiveEntity;

  private OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse> activeMessageHandler;
  private OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse> passiveMessageHandler;

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

    ClusteringServiceConfiguration configuration =
      ClusteringServiceConfigurationBuilder.cluster(URI.create(STRIPE_URI))
        .autoCreate(c -> c)
        .build();

    service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
      newResourcePoolsBuilder().with(clusteredDedicated("test", 2, org.ehcache.config.units.MemoryUnit.MB))).build();

    @SuppressWarnings("unchecked")
    ClusteringService.ClusteredCacheIdentifier spaceIdentifier = (ClusteringService.ClusteredCacheIdentifier) service.getPersistenceSpaceIdentifier("test", config);

    storeProxy = service.getServerStoreProxy(spaceIdentifier, new StoreConfigurationImpl<>(config, 1, null, null), Consistency.STRONG, mock(ServerCallback.class));

    activeEntity = observableClusterTierServerEntityService.getServedActiveEntitiesFor("test").get(0);
    passiveEntity = observableClusterTierServerEntityService.getServedPassiveEntitiesFor("test").get(0);

    activeMessageHandler = activeEntity.getMessageHandler();
    passiveMessageHandler = passiveEntity.getMessageHandler();
  }

  @After
  public void tearDown() throws Exception {
    if(service.isConnected()) {
      service.stop();
    }
    UnitTestConnectionService.removeStripe(STRIPENAME);
    clusterControl.tearDown();
  }

  @Test
  public void messageTrackedAndRemovedWhenClientLeaves() throws Exception {
    assertThat(activeMessageHandler.getTrackedClients().count()).isZero(); // no client tracked

    storeProxy.getAndAppend(42L, createPayload(42L));

    assertThat(activeMessageHandler.getRecordedMessages().collect(Collectors.toList())).hasSize(1); // should now track one message

    assertThat(activeEntity.getConnectedClients()).hasSize(1); // make sure we currently have one client attached

    service.stop(); // stop the service. It will remove the client

    activeEntity.notifyDestroyed(activeMessageHandler.getTrackedClients().iterator().next()); // Notify that the client was removed. A real clustered server will do that. But the Passthrough doesn't. So we simulate it

    waitForPredicate(e -> activeEntity.getConnectedClients().size() > 0, 2000); // wait for the client to be removed, might be async, so we wait

    assertThat(activeMessageHandler.getTrackedClients().count()).isZero(); // all tracked messages for this client should have been removed
  }

  @Test
  public void untrackedMessageAreNotStored() throws Exception {
    // Nothing tracked
    assertThat(activeMessageHandler.getTrackedClients().count()).isZero();

    // Send a replace message, those are not tracked
    storeProxy.replaceAtHead(44L, chainOf(createPayload(44L)), chainOf());

    // Not tracked as well
    storeProxy.get(42L);

    assertThat(activeMessageHandler.getTrackedClients().count()).isZero();
  }

  @Test
  public void trackedMessagesReplicatedToPassive() throws Exception {
    clusterControl.terminateOnePassive();

    storeProxy.getAndAppend(42L, createPayload(42L));

    clusterControl.startOneServer();
    clusterControl.waitForRunningPassivesInStandby();

    // Save the new handler from the freshly started passive
    passiveEntity = observableClusterTierServerEntityService.getServedPassiveEntitiesFor("test").get(1);
    passiveMessageHandler = passiveEntity.getMessageHandler();

    assertThat(passiveMessageHandler.getTrackedClients().count()).isEqualTo(1L); // one client tracked

    Map<Long, EhcacheEntityResponse> responses = activeMessageHandler.getRecordedMessages().filter(r->r.getClientSourceId().toLong() == activeMessageHandler.getTrackedClients().findFirst().get().toLong())
            .collect(Collectors.toMap(r->r.getTransactionId(), r->r.getResponse()));
    assertThat(responses).hasSize(1); // one message should have sync
  }

  @Test
  public void messageTrackedAndRemovedByPassiveWhenClientLeaves() throws Exception {
    assertThat(passiveMessageHandler.getTrackedClients().count()).isZero(); // nothing tracked right now

    storeProxy.getAndAppend(42L, createPayload(42L));

    Map<Long, EhcacheEntityResponse> responses = activeMessageHandler.getRecordedMessages().filter(r->r.getClientSourceId().toLong() == activeMessageHandler.getTrackedClients().findFirst().get().toLong())
            .collect(Collectors.toMap(r->r.getTransactionId(), r->r.getResponse()));
    assertThat(responses).hasSize(1); // should now track one message

    service.stop(); // stop the service. It will remove the client

    activeEntity.notifyDestroyed(passiveMessageHandler.getTrackedClients().iterator().next());
    passiveEntity.notifyDestroyed(passiveMessageHandler.getTrackedClients().iterator().next()); // Notify that the client was removed. A real clustered server will do that. But the Passthrough doesn't. So we simulate it

    waitForPredicate(e -> activeEntity.getConnectedClients().size() > 0, 2000); // wait for the client to be removed, might be async, so we wait

    assertThat(passiveMessageHandler.getTrackedClients().count()).isZero(); // all tracked messages for this client should have been removed
  }

  private <T> void waitForPredicate(Predicate<T> predicate, long timeoutInMs) {
    long now = System.currentTimeMillis();
    while(predicate.test(null)) {
      if((System.currentTimeMillis() - now) > timeoutInMs) {
        fail("Waiting for attached client to disconnect timed-out");
      }
    }
  }
}
