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

package org.ehcache.clustered.server.store;

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.PoolAllocation.Dedicated;
import org.ehcache.clustered.common.PoolAllocation.Shared;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerStoreConfigurationException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.messages.ConcurrentEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponseFactory;
import org.ehcache.clustered.common.internal.messages.EhcacheResponseType;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageFactory;
import org.ehcache.clustered.common.internal.messages.ServerStoreMessageFactory;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.store.ClusterTierEntityConfiguration;
import org.ehcache.clustered.server.ConcurrencyStrategies;
import org.ehcache.clustered.server.EhcacheStateServiceImpl;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.ServerSideServerStore;
import org.ehcache.clustered.server.ServerStoreEvictionListener;
import org.ehcache.clustered.server.internal.messages.EhcacheDataSyncMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage;
import org.ehcache.clustered.server.state.ClientMessageTracker;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.InvalidationTracker;
import org.ehcache.clustered.server.state.config.EhcacheStoreStateServiceConfig;
import org.ehcache.clustered.server.store.ClusterTierActiveEntity.InvalidationHolder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.management.service.monitoring.ActiveEntityMonitoringServiceConfiguration;
import org.terracotta.management.service.monitoring.ConsumerManagementRegistryConfiguration;
import org.terracotta.management.service.monitoring.EntityMonitoringService;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResourceIdentifier;
import org.terracotta.offheapresource.OffHeapResources;
import org.terracotta.offheapstore.util.MemoryUnit;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ClusterTierActiveEntityTest {

  private static final LifeCycleMessageFactory MESSAGE_FACTORY = new LifeCycleMessageFactory();
  private static final UUID CLIENT_ID = UUID.randomUUID();
  private static final KeySegmentMapper DEFAULT_MAPPER = new KeySegmentMapper(16);

  private String defaultStoreName = "store";
  private String defaultResource = "default";
  private String defaultSharedPool = "defaultShared";
  private String identifier = "identifier";
  private OffHeapIdentifierRegistry defaultRegistry;
  private ServerStoreConfiguration defaultStoreConfiguration;
  private ClusterTierEntityConfiguration defaultConfiguration;

  @Before
  public void setUp() {
    MESSAGE_FACTORY.setClientId(CLIENT_ID);
    defaultRegistry = new OffHeapIdentifierRegistry();
    defaultRegistry.addResource(defaultResource, 10, MemoryUnit.MEGABYTES);
    defaultStoreConfiguration = new ServerStoreConfigBuilder().dedicated(defaultResource, 1024, MemoryUnit.KILOBYTES).build();
    defaultConfiguration = new ClusterTierEntityConfiguration(identifier, defaultStoreName,
      defaultStoreConfiguration);
  }

  @Test(expected = ConfigurationException.class)
  public void testConfigNull() throws Exception {
    new ClusterTierActiveEntity(mock(ServiceRegistry.class), null, DEFAULT_MAPPER);
  }

  @Test
  public void testConnected() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    Set<ClientDescriptor> connectedClients = activeEntity.getConnectedClients();
    assertThat(connectedClients, hasSize(1));
    assertThat(connectedClients, hasItem(client));
  }

  @Test
  public void testConnectedAgain() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    activeEntity.connected(client);
    Set<ClientDescriptor> connectedClients = activeEntity.getConnectedClients();
    assertThat(connectedClients, hasSize(1));
    assertThat(connectedClients, hasItem(client));
  }

  @Test
  public void testConnectedSecond() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);

    ClientDescriptor client1 = new TestClientDescriptor();
    activeEntity.connected(client1);

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);

    Set<ClientDescriptor> connectedClients = activeEntity.getConnectedClients();
    assertThat(connectedClients, hasSize(2));
    assertThat(connectedClients, hasItems(client1, client2));
  }

  @Test
  public void testDisconnectedNotConnected() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);

    ClientDescriptor client1 = new TestClientDescriptor();
    activeEntity.disconnected(client1);
    // Not expected to fail ...
  }

  /**
   * Ensures the disconnect of a connected client is properly tracked.
   */
  @Test
  public void testDisconnected() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);

    ClientDescriptor client1 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.disconnected(client1);

    assertThat(activeEntity.getConnectedClients(), hasSize(0));
  }

  /**
   * Ensures the disconnect of a connected client is properly tracked and does not affect others.
   */
  @Test
  public void testDisconnectedSecond() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);

    ClientDescriptor client1 = new TestClientDescriptor();
    activeEntity.connected(client1);

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);

    assertThat(activeEntity.getConnectedClients(), hasSize(2));

    activeEntity.disconnected(client1);

    Set<ClientDescriptor> connectedClients = activeEntity.getConnectedClients();
    assertThat(connectedClients, hasSize(1));
    assertThat(connectedClients, hasItem(client2));
  }

  @Test
  public void testLoadExistingRegistersEvictionListener() throws Exception {
    EhcacheStateService stateService = mock(EhcacheStateService.class);
    when(stateService.getClientMessageTracker(anyString())).thenReturn(mock(ClientMessageTracker.class));

    ServerSideServerStore store = mock(ServerSideServerStore.class);
    when(stateService.loadStore(eq(defaultStoreName), any())).thenReturn(store);

    IEntityMessenger entityMessenger = mock(IEntityMessenger.class);
    ServiceRegistry registry = getCustomMockedServiceRegistry(stateService, null, entityMessenger, null);
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(registry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.loadExisting();
    verify(store).setEvictionListener(any(ServerStoreEvictionListener.class));
  }

  @Test
  public void testAppendInvalidationAcksTakenIntoAccount() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client1 = new TestClientDescriptor();
    ClientDescriptor client2 = new TestClientDescriptor();
    ClientDescriptor client3 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.connected(client2);
    activeEntity.connected(client3);

    UUID client2Id = UUID.randomUUID();
    UUID client3Id = UUID.randomUUID();

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(CLIENT_ID);

    // attach to the store
    MESSAGE_FACTORY.setClientId(CLIENT_ID);
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );
    MESSAGE_FACTORY.setClientId(client2Id);
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );
    MESSAGE_FACTORY.setClientId(client3Id);
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );

    // perform an append
    assertSuccess(
        activeEntity.invoke(client1, messageFactory.appendOperation(1L, createPayload(1L)))
    );

    // assert that an invalidation request is pending
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    InvalidationHolder invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(2));
    assertThat(invalidationHolder.clientsHavingToInvalidate, containsInAnyOrder(client2, client3));

    // client 2 acks
    assertSuccess(
        activeEntity.invoke(client2, messageFactory.clientInvalidationAck(1L, activeEntity.getClientsWaitingForInvalidation().keySet().iterator().next()))
    );

    // assert that client 2 is not waited for anymore
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(1));
    assertThat(invalidationHolder.clientsHavingToInvalidate, contains(client3));

    // client 3 acks
    assertSuccess(
        activeEntity.invoke(client3, messageFactory.clientInvalidationAck(1L, activeEntity.getClientsWaitingForInvalidation().keySet().iterator().next()))
    );

    // assert that the invalidation request is done since all clients disconnected
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(0));
  }

  @Test
  public void testClearInvalidationAcksTakenIntoAccount() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client1 = new TestClientDescriptor();
    ClientDescriptor client2 = new TestClientDescriptor();
    ClientDescriptor client3 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.connected(client2);
    activeEntity.connected(client3);

    UUID client2Id = UUID.randomUUID();
    UUID client3Id = UUID.randomUUID();

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(CLIENT_ID);

    // attach to the store
    MESSAGE_FACTORY.setClientId(CLIENT_ID);
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );
    MESSAGE_FACTORY.setClientId(client2Id);
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );
    MESSAGE_FACTORY.setClientId(client3Id);
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );

    // perform a clear
    assertSuccess(
        activeEntity.invoke(client1, messageFactory.clearOperation())
    );

    // assert that an invalidation request is pending
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    InvalidationHolder invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(2));
    assertThat(invalidationHolder.clientsHavingToInvalidate, containsInAnyOrder(client2, client3));

    // client 2 acks
    assertSuccess(
        activeEntity.invoke(client2, messageFactory.clientInvalidationAllAck(activeEntity.getClientsWaitingForInvalidation().keySet().iterator().next()))
    );

    // assert that client 2 is not waited for anymore
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(1));
    assertThat(invalidationHolder.clientsHavingToInvalidate, contains(client3));

    // client 3 acks
    assertSuccess(
        activeEntity.invoke(client3, messageFactory.clientInvalidationAllAck(activeEntity.getClientsWaitingForInvalidation().keySet().iterator().next()))
    );

    // assert that the invalidation request is done since all clients disconnected
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(0));
  }

  @Test
  public void testAppendInvalidationDisconnectionOfInvalidatingClientsTakenIntoAccount() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client1 = new TestClientDescriptor();
    ClientDescriptor client2 = new TestClientDescriptor();
    ClientDescriptor client3 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.connected(client2);
    activeEntity.connected(client3);

    UUID client2Id = UUID.randomUUID();
    UUID client3Id = UUID.randomUUID();

    // attach to the store
    MESSAGE_FACTORY.setClientId(CLIENT_ID);
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );
    MESSAGE_FACTORY.setClientId(client2Id);
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );
    MESSAGE_FACTORY.setClientId(client3Id);
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(CLIENT_ID);

    // perform an append
    assertSuccess(
        activeEntity.invoke(client1, messageFactory.appendOperation(1L, createPayload(1L)))
    );

    // assert that an invalidation request is pending
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    InvalidationHolder invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(2));
    assertThat(invalidationHolder.clientsHavingToInvalidate, containsInAnyOrder(client2, client3));

    // disconnect client2
    activeEntity.disconnected(client2);

    // assert that client 2 is not waited for anymore
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(1));
    assertThat(invalidationHolder.clientsHavingToInvalidate, contains(client3));

    // disconnect client3
    activeEntity.disconnected(client3);

    // assert that the invalidation request is done since all clients disconnected
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(0));
  }

  @Test
  public void testClearInvalidationDisconnectionOfInvalidatingClientsTakenIntoAccount() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client1 = new TestClientDescriptor();
    ClientDescriptor client2 = new TestClientDescriptor();
    ClientDescriptor client3 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.connected(client2);
    activeEntity.connected(client3);

    UUID client2Id = UUID.randomUUID();
    UUID client3Id = UUID.randomUUID();

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(CLIENT_ID);

    // attach to the store
    MESSAGE_FACTORY.setClientId(CLIENT_ID);
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );
    MESSAGE_FACTORY.setClientId(client2Id);
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );
    MESSAGE_FACTORY.setClientId(client3Id);
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );

    // perform an append
    assertSuccess(
        activeEntity.invoke(client1, messageFactory.clearOperation())
    );

    // assert that an invalidation request is pending
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    InvalidationHolder invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(2));
    assertThat(invalidationHolder.clientsHavingToInvalidate, containsInAnyOrder(client2, client3));

    // disconnect client2
    activeEntity.disconnected(client2);

    // assert that client 2 is not waited for anymore
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(1));
    assertThat(invalidationHolder.clientsHavingToInvalidate, contains(client3));

    // disconnect client3
    activeEntity.disconnected(client3);

    // assert that the invalidation request is done since all clients disconnected
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(0));
  }

  @Test
  public void testAppendInvalidationDisconnectionOfBlockingClientTakenIntoAccount() throws Exception {
    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .dedicated(defaultResource, 4, MemoryUnit.MEGABYTES)
        .consistency(Consistency.STRONG)
        .build();
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry,
      new ClusterTierEntityConfiguration(identifier, defaultStoreName, serverStoreConfiguration), DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client1 = new TestClientDescriptor();
    ClientDescriptor client2 = new TestClientDescriptor();
    ClientDescriptor client3 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.connected(client2);
    activeEntity.connected(client3);

    UUID client2Id = UUID.randomUUID();
    UUID client3Id = UUID.randomUUID();

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(CLIENT_ID);

    // attach to the store
    MESSAGE_FACTORY.setClientId(CLIENT_ID);
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateServerStore(defaultStoreName, serverStoreConfiguration))
    );
    MESSAGE_FACTORY.setClientId(client2Id);
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore(defaultStoreName, serverStoreConfiguration))
    );
    MESSAGE_FACTORY.setClientId(client3Id);
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateServerStore(defaultStoreName, serverStoreConfiguration))
    );

    // perform an append
    assertSuccess(
        activeEntity.invoke(client1, messageFactory.appendOperation(1L, createPayload(1L)))
    );

    // assert that an invalidation request is pending
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    InvalidationHolder invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(2));
    assertThat(invalidationHolder.clientsHavingToInvalidate, containsInAnyOrder(client2, client3));

    // disconnect client1
    activeEntity.disconnected(client1);

    // assert that the invalidation request is done since the originating client disconnected
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(0));
  }

  @Test
  public void testClearInvalidationDisconnectionOfBlockingClientTakenIntoAccount() throws Exception {
    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .dedicated(defaultResource, 4, MemoryUnit.MEGABYTES)
        .consistency(Consistency.STRONG)
        .build();
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry,
      new ClusterTierEntityConfiguration(identifier, defaultStoreName, serverStoreConfiguration), DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client1 = new TestClientDescriptor();
    ClientDescriptor client2 = new TestClientDescriptor();
    ClientDescriptor client3 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.connected(client2);
    activeEntity.connected(client3);

    UUID client2Id = UUID.randomUUID();
    UUID client3Id = UUID.randomUUID();

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(CLIENT_ID);

    // attach to the store
    MESSAGE_FACTORY.setClientId(CLIENT_ID);
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateServerStore(defaultStoreName, serverStoreConfiguration))
    );
    MESSAGE_FACTORY.setClientId(client2Id);
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore(defaultStoreName, serverStoreConfiguration))
    );
    MESSAGE_FACTORY.setClientId(client3Id);
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateServerStore(defaultStoreName, serverStoreConfiguration))
    );

    // perform an append
    assertSuccess(
        activeEntity.invoke(client1, messageFactory.clearOperation())
    );

    // assert that an invalidation request is pending
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    InvalidationHolder invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(2));
    assertThat(invalidationHolder.clientsHavingToInvalidate, containsInAnyOrder(client2, client3));

    // disconnect client1
    activeEntity.disconnected(client1);

    // assert that the invalidation request is done since the originating client disconnected
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(0));
  }

  @Test
  public void testConnectedButNotAttachedClientFailsInvokingServerStoreOperation() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(CLIENT_ID);

    assertFailure(
        activeEntity.invoke(client, messageFactory.appendOperation(1L, createPayload(1L))),
        LifecycleException.class, "Client not attached to cluster tier 'store'"
    );
  }

  @Test
  public void testWithAttachmentSucceedsInvokingServerStoreOperation() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(CLIENT_ID);

    // attach to the store
    assertSuccess(
        activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );

    assertSuccess(
        activeEntity.invoke(client, messageFactory.appendOperation(1L, createPayload(1L)))
    );

    EhcacheEntityResponse response = activeEntity.invoke(client, messageFactory.getOperation(1L));
    assertThat(response, instanceOf(EhcacheEntityResponse.GetResponse.class));
    EhcacheEntityResponse.GetResponse getResponse = (EhcacheEntityResponse.GetResponse) response;
    assertThat(getResponse.getChain().isEmpty(), is(false));
  }

  @Test
  public void testCreateDedicatedServerStore() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    assertThat(defaultRegistry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder(defaultStoreName));

    assertThat(defaultRegistry.getResource(defaultResource).getUsed(), is(MemoryUnit.MEGABYTES.toBytes(1L)));

    assertThat(activeEntity.getConnectedClients(), empty());
    assertThat(defaultRegistry.getStoreManagerService().getStores(), containsInAnyOrder(defaultStoreName));

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    assertSuccess(
        activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration))
    );

    assertThat(activeEntity.getAttachedClients(), contains(client));

    /*
     * Ensure the dedicated resource pool remains after client disconnect.
     */
    activeEntity.disconnected(client);

    assertThat(defaultRegistry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder(defaultStoreName));

    assertThat(activeEntity.getConnectedClients(), empty());
    assertThat(defaultRegistry.getStoreManagerService().getStores(), containsInAnyOrder(defaultStoreName));
  }

  @Test
  public void testCreateDedicatedServerStoreExisting() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClusterTierActiveEntity otherEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    try {
      otherEntity.createNew();
      fail("Duplicate creation should fail with an exception");
    } catch (ConfigurationException e) {
      assertThat(e.getMessage(), containsString("already exists"));
    }
  }

  @Test
  public void testValidateDedicatedServerStore() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);

    UUID client2Id = UUID.randomUUID();

    MESSAGE_FACTORY.setClientId(client2Id);

    assertSuccess(activeEntity.invoke(client2,
        MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration)));

    assertThat(defaultRegistry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder(defaultStoreName));

    assertThat(defaultRegistry.getResource(defaultResource).getUsed(), is(MemoryUnit.MEGABYTES.toBytes(1L)));

    assertThat(activeEntity.getConnectedClients(), hasSize(2));
    assertThat(activeEntity.getAttachedClients(), contains(client2));
    assertThat(defaultRegistry.getStoreManagerService().getStores(), containsInAnyOrder(defaultStoreName));
  }

  @Test
  public void testValidateDedicatedServerStoreBad() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.validateServerStore(defaultStoreName,
            new ServerStoreConfigBuilder()
                .dedicated(defaultResource, 8, MemoryUnit.MEGABYTES)
                .build())),
        InvalidServerStoreConfigurationException.class);

  }

  @Test
  public void testValidateUnknown() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName,
      new ServerStoreConfigBuilder().unknown().build())));
  }

  @Test
  public void testCreateSharedServerStore() throws Exception {
    defaultRegistry.addSharedPool(defaultSharedPool, MemoryUnit.MEGABYTES.toBytes(2), defaultResource);
    ServerStoreConfiguration storeConfiguration = new ServerStoreConfigBuilder()
      .shared(defaultSharedPool)
      .build();
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry,
      new ClusterTierEntityConfiguration(identifier, defaultStoreName, storeConfiguration), DEFAULT_MAPPER);
    activeEntity.createNew();


    assertThat(defaultRegistry.getStoreManagerService().getStores(), containsInAnyOrder(defaultStoreName));

    assertThat(defaultRegistry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder(defaultSharedPool));
    assertThat(defaultRegistry.getStoreManagerService().getDedicatedResourcePoolIds(), empty());

    assertThat(defaultRegistry.getResource(defaultResource).getUsed(), is(MemoryUnit.MEGABYTES.toBytes(2L)));

  }

  @Test
  public void testCreateSharedServerStoreExisting() throws Exception {
    defaultRegistry.addSharedPool(defaultSharedPool, MemoryUnit.MEGABYTES.toBytes(2), defaultResource);
    ServerStoreConfiguration storeConfiguration = new ServerStoreConfigBuilder()
      .shared(defaultSharedPool)
      .build();
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry,
      new ClusterTierEntityConfiguration(identifier, defaultStoreName, storeConfiguration), DEFAULT_MAPPER);
    activeEntity.createNew();

    ClusterTierActiveEntity otherEntity = new ClusterTierActiveEntity(defaultRegistry,
      new ClusterTierEntityConfiguration(identifier, defaultStoreName, storeConfiguration), DEFAULT_MAPPER);
    try {
      otherEntity.createNew();
      fail("Duplicate creation should fail with an exception");
    } catch (ConfigurationException e) {
      assertThat(e.getMessage(), containsString("already exists"));
    }
  }

  @Test
  public void testValidateSharedServerStore() throws Exception {
    defaultRegistry.addSharedPool(defaultSharedPool, MemoryUnit.MEGABYTES.toBytes(2), defaultResource);
    ServerStoreConfiguration storeConfiguration = new ServerStoreConfigBuilder()
      .shared(defaultSharedPool)
      .build();
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry,
      new ClusterTierEntityConfiguration(identifier, defaultStoreName, storeConfiguration), DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName, storeConfiguration)));

    assertThat(activeEntity.getAttachedClients(), contains(client));
  }

  @Test
  public void testValidateServerStore_DedicatedStoresDifferentSizes() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    ServerStoreConfiguration storeConfiguration = new ServerStoreConfigBuilder()
      .dedicated(defaultResource, 2, MemoryUnit.MEGABYTES)
      .build();

    String expectedMessageContent = "Existing ServerStore configuration is not compatible with the desired configuration: " +
                                    "\n\t" +
                                    "resourcePoolType existing: " +
                                    defaultStoreConfiguration.getPoolAllocation() +
                                    ", desired: " +
                                    storeConfiguration.getPoolAllocation();

    assertFailure(activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName, storeConfiguration)),
      InvalidServerStoreConfigurationException.class, expectedMessageContent);
  }

  @Test
  public void testValidateServerStore_DedicatedStoreResourceNamesDifferent() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    ServerStoreConfiguration storeConfiguration = new ServerStoreConfigBuilder()
      .dedicated("otherResource", 1, MemoryUnit.MEGABYTES)
      .build();

    String expectedMessageContent = "Existing ServerStore configuration is not compatible with the desired configuration: " +
                                    "\n\t" +
                                    "resourcePoolType existing: " +
                                    defaultStoreConfiguration.getPoolAllocation() +
                                    ", desired: " +
                                    storeConfiguration.getPoolAllocation();

    assertFailure(activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName, storeConfiguration)),
      InvalidServerStoreConfigurationException.class, expectedMessageContent);
  }

  @Test
  public void testValidateServerStore_DifferentSharedPools() throws Exception {
    defaultRegistry.addSharedPool(defaultSharedPool, MemoryUnit.MEGABYTES.toBytes(2), defaultResource);
    ServerStoreConfiguration storeConfiguration = new ServerStoreConfigBuilder()
      .shared(defaultSharedPool)
      .build();
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry,
      new ClusterTierEntityConfiguration(identifier, defaultStoreName, storeConfiguration), DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    ServerStoreConfiguration otherConfiguration = new ServerStoreConfigBuilder()
      .shared("other")
      .build();

    String expectedMessageContent = "Existing ServerStore configuration is not compatible with the desired configuration: " +
                                    "\n\t" +
                                    "resourcePoolType existing: " +
                                    storeConfiguration.getPoolAllocation() +
                                    ", desired: " +
                                    otherConfiguration.getPoolAllocation();

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.validateServerStore(defaultStoreName,
          otherConfiguration)),InvalidServerStoreConfigurationException.class,expectedMessageContent);
  }

  @Test
  public void testDestroyServerStore() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    activeEntity.destroy();

    assertThat(defaultRegistry.getResource(defaultResource).getUsed(), is(0L));

    assertThat(defaultRegistry.getStoreManagerService().getStores(), empty());
    assertThat(defaultRegistry.getStoreManagerService().getDedicatedResourcePoolIds(), empty());
  }

  /**
   * Ensures shared pool and store (cache) name spaces are independent.
   * The cache alias is used as the name for a {@code ServerStore} instance; this name can be
   * the same as, but is independent of, the shared pool name.  The
   */
  @Test
  public void testSharedPoolCacheNameCollision() throws Exception {
    defaultRegistry.addSharedPool(defaultStoreName, MemoryUnit.MEGABYTES.toBytes(2), defaultResource);

    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    assertThat(defaultRegistry.getStoreManagerService().getSharedResourcePoolIds(), contains(defaultStoreName));
    assertThat(defaultRegistry.getStoreManagerService().getDedicatedResourcePoolIds(), contains(defaultStoreName));
    assertThat(defaultRegistry.getStoreManagerService().getStores(), containsInAnyOrder(defaultStoreName));
  }

  @Test
  public void testCreateNonExistentSharedPool() throws Exception {
    ServerStoreConfiguration storeConfiguration = new ServerStoreConfigBuilder()
      .shared(defaultSharedPool)
      .build();

    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry,
      new ClusterTierEntityConfiguration(identifier, defaultStoreName, storeConfiguration), DEFAULT_MAPPER);
    try {
      activeEntity.createNew();
      fail("Creation with non-existent shared pool should have failed");
    } catch (ConfigurationException e) {
      assertThat(e.getMessage(), containsString("undefined"));
    }
  }

  @Test
  public void testCreateUnknownServerResource() throws Exception {
    ServerStoreConfiguration storeConfiguration = new ServerStoreConfigBuilder()
      .dedicated("unknown", 2, MemoryUnit.MEGABYTES)
      .build();
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry,
      new ClusterTierEntityConfiguration(identifier, defaultStoreName, storeConfiguration), DEFAULT_MAPPER);
    try {
      activeEntity.createNew();
      fail("Creation with non-existent shared pool should have failed");
    } catch (ConfigurationException e) {
      assertThat(e.getMessage(), containsString("Non-existent server side resource"));
    }
  }

  @Test
  public void testSyncToPassiveNoData() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);


    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration)));

    @SuppressWarnings("unchecked")
    PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel = mock(PassiveSynchronizationChannel.class);
    activeEntity.synchronizeKeyToPassive(syncChannel, 3);

    verifyZeroInteractions(syncChannel);
  }

  @Test
  public void testSyncToPassiveBatchedByDefault() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);


    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration)));

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(UUID.randomUUID());

    ByteBuffer payload = ByteBuffer.allocate(512);
    // Put keys that maps to the same concurrency key
    assertSuccess(activeEntity.invoke(client, messageFactory.appendOperation(1L, payload)));
    assertSuccess(activeEntity.invoke(client, messageFactory.appendOperation(-2L, payload)));
    assertSuccess(activeEntity.invoke(client, messageFactory.appendOperation(17L, payload)));

    @SuppressWarnings("unchecked")
    PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel = mock(PassiveSynchronizationChannel.class);
    activeEntity.synchronizeKeyToPassive(syncChannel, 3);

    verify(syncChannel).synchronizeToPassive(any(EhcacheDataSyncMessage.class));
  }

  @Test
  public void testDataSyncToPassiveCustomBatchSize() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);


    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration)));

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(UUID.randomUUID());

    ByteBuffer payload = ByteBuffer.allocate(512);
    // Put keys that maps to the same concurrency key
    ServerStoreOpMessage.AppendMessage testMessage = messageFactory.appendOperation(1L, payload);
    activeEntity.invoke(client, testMessage);
    activeEntity.invoke(client, messageFactory.appendOperation(-2L, payload));
    activeEntity.invoke(client, messageFactory.appendOperation(17L, payload));
    activeEntity.invoke(client, messageFactory.appendOperation(33L, payload));

    System.setProperty(ClusterTierActiveEntity.SYNC_DATA_SIZE_PROP, "512");
    ConcurrencyStrategies.DefaultConcurrencyStrategy concurrencyStrategy = new ConcurrencyStrategies.DefaultConcurrencyStrategy(DEFAULT_MAPPER);
    int concurrencyKey = concurrencyStrategy.concurrencyKey(testMessage);
    try {
      @SuppressWarnings("unchecked")
      PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel = mock(PassiveSynchronizationChannel.class);
      activeEntity.synchronizeKeyToPassive(syncChannel, concurrencyKey);

      verify(syncChannel, atLeast(2)).synchronizeToPassive(any(EhcacheDataSyncMessage.class));
    } finally {
      System.clearProperty(ClusterTierActiveEntity.SYNC_DATA_SIZE_PROP);
    }
  }

  @Test
  public void testLoadExistingRecoversInflightInvalidationsForEventualCache() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    EhcacheStateServiceImpl ehcacheStateService = defaultRegistry.getStoreManagerService();
    ehcacheStateService.createStore(defaultStoreName, defaultStoreConfiguration, false);  //Passive would have done this before failover

    InvalidationTracker invalidationTracker = ehcacheStateService.getInvalidationTracker(defaultStoreName);

    Random random = new Random();
    random.ints(0, 100).limit(10).forEach(x -> invalidationTracker.trackHashInvalidation(x));

    activeEntity.loadExisting();

    assertThat(activeEntity.getInflightInvalidations().isEmpty(), is(false));
  }

  @Test
  public void testPromotedActiveIgnoresDuplicateMessages() throws Exception {
    defaultRegistry.getService(new EhcacheStoreStateServiceConfig(identifier, DEFAULT_MAPPER));
    EhcacheStateServiceImpl ehcacheStateService = defaultRegistry.getStoreManagerService();
    ehcacheStateService.createStore(defaultStoreName, defaultStoreConfiguration, false);  //Passive would have done this before failover

    ClientMessageTracker clientMessageTracker = ehcacheStateService.getClientMessageTracker(defaultStoreName);

    Random random = new Random();
    Set<Long> msgIds = new HashSet<>();
    random.longs(100).distinct().forEach(x -> {
      msgIds.add(x);
    });

    Set<Long> applied = new HashSet<>();
    msgIds.stream().limit(80).forEach(x -> {
      applied.add(x);
      clientMessageTracker.applied(x, CLIENT_ID);
    });

    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration));

    activeEntity.loadExisting();

    IEntityMessenger entityMessenger = defaultRegistry.getEntityMessenger();

    ServerStoreMessageFactory serverStoreMessageFactory = new ServerStoreMessageFactory(CLIENT_ID);
    EhcacheEntityResponseFactory entityResponseFactory = new EhcacheEntityResponseFactory();
    applied.forEach(y -> {
      EhcacheEntityMessage message = serverStoreMessageFactory.appendOperation(y, createPayload(y));
      message.setId(y);
      assertThat(activeEntity.invoke(client, message), is(entityResponseFactory.success()));
    });

    verify(entityMessenger, times(0)).messageSelfAndDeferRetirement(any(), any());
  }

  @Test
  public void testReplicationMessageAndOriginalServerStoreOpMessageHasSameConcurrency() throws Exception {

    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    IEntityMessenger entityMessenger = defaultRegistry.getEntityMessenger();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration)));

    reset(entityMessenger);
    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(CLIENT_ID);
    EhcacheEntityMessage getAndAppend = messageFactory.getAndAppendOperation(1L, createPayload(1L));
    activeEntity.invoke(client, getAndAppend);

    ArgumentCaptor<PassiveReplicationMessage.ChainReplicationMessage> captor = ArgumentCaptor.forClass(PassiveReplicationMessage.ChainReplicationMessage.class);
    verify(entityMessenger).messageSelfAndDeferRetirement(any(), captor.capture());
    PassiveReplicationMessage.ChainReplicationMessage replicatedMessage = captor.getValue();

    assertThat(replicatedMessage.concurrencyKey(), is(((ConcurrentEntityMessage) getAndAppend).concurrencyKey()));
  }

  @Test
  public void testInvalidMessageThrowsError() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    try {
      activeEntity.invoke(client, new InvalidMessage());
      fail("Invalid message should result in AssertionError");
    } catch (AssertionError e) {
      assertThat(e.getMessage(), containsString("Unsupported"));
    }
  }

  @Test
  public void testActiveDoesNotTrackMessagesByDefault() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    activeEntity.createNew();

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(CLIENT_ID);

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration)));

    ServerStoreOpMessage.AppendMessage message = messageFactory.appendOperation(1L, createPayload(1L));
    message.setId(123);
    activeEntity.invoke(client, message);

    // create another message that has the same message ID
    message = messageFactory.appendOperation(2L, createPayload(1L));
    message.setId(123);

    activeEntity.invoke(client, message); // this invoke should be rejected due to duplicate message id

    ServerStoreOpMessage.GetMessage getMessage = messageFactory.getOperation(2L);
    EhcacheEntityResponse.GetResponse response = (EhcacheEntityResponse.GetResponse) activeEntity.invoke(client, getMessage);
    assertThat(response.getChain().isEmpty(), is(false));
  }

  @Test
  public void testActiveMessageTracking() throws Exception {
    ClusterTierActiveEntity activeEntity = new ClusterTierActiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    EhcacheStateServiceImpl ehcacheStateService = defaultRegistry.getStoreManagerService();
    ehcacheStateService.createStore(defaultStoreName, defaultStoreConfiguration, false);  //hack to enable message tracking on active

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory(CLIENT_ID);

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore(defaultStoreName, defaultStoreConfiguration)));

    ServerStoreOpMessage.AppendMessage message = messageFactory.appendOperation(1L, createPayload(1L));
    message.setId(123);
    activeEntity.invoke(client, message);

    // create another message that has the same message ID
    message = messageFactory.appendOperation(2L, createPayload(1L));
    message.setId(123);

    activeEntity.invoke(client, message); // this invoke should be rejected due to duplicate message id

    ServerStoreOpMessage.GetMessage getMessage = messageFactory.getOperation(2L);
    EhcacheEntityResponse.GetResponse response = (EhcacheEntityResponse.GetResponse) activeEntity.invoke(client, getMessage);
    assertThat(response.getChain().isEmpty(), is(true));
  }

  private void assertSuccess(EhcacheEntityResponse response) throws Exception {
    if (!response.equals(EhcacheEntityResponse.Success.INSTANCE)) {
      throw ((EhcacheEntityResponse.Failure) response).getCause();
    }
  }

  private void assertFailure(EhcacheEntityResponse response, Class<? extends Exception> expectedException) {
    assertThat(response.getResponseType(), is(EhcacheResponseType.FAILURE));
    assertThat(((EhcacheEntityResponse.Failure) response).getCause(), is(instanceOf(expectedException)));
  }

  private void assertFailure(EhcacheEntityResponse response, Class<? extends Exception> expectedException, String expectedMessageContent) {
    assertThat(response.getResponseType(), is(EhcacheResponseType.FAILURE));
    Exception cause = ((EhcacheEntityResponse.Failure) response).getCause();
    assertThat(cause, is(instanceOf(expectedException)));
    assertThat(cause.getMessage(), containsString(expectedMessageContent));
  }

  @SuppressWarnings("unchecked")
  ServiceRegistry getCustomMockedServiceRegistry(EhcacheStateService stateService, ClientCommunicator clientCommunicator,
                                                 IEntityMessenger entityMessenger, EntityMonitoringService entityMonitoringService) {
    return new ServiceRegistry() {
      @Override
      public <T> T getService(final ServiceConfiguration<T> configuration) {
        Class<T> serviceType = configuration.getServiceType();
        if (serviceType.isAssignableFrom(ClientCommunicator.class)) {
          return (T) clientCommunicator;
        } else if (serviceType.isAssignableFrom(IEntityMessenger.class)) {
          return (T) entityMessenger;
        } else if (serviceType.isAssignableFrom(EhcacheStateService.class)) {
          return (T) stateService;
        } else if (serviceType.isAssignableFrom(EntityMonitoringService.class)) {
          return (T) entityMonitoringService;
        }
        throw new AssertionError("Unknown service configuration of type: " + serviceType);
      }
    };
  }

  /**
   * Builder for {@link ServerStoreConfiguration} instances.
   */
  private static final class ServerStoreConfigBuilder {
    private PoolAllocation poolAllocation;
    private String storedKeyType = "java.lang.Long";
    private String storedValueType = "java.lang.String";
    private String keySerializerType;
    private String valueSerializerType;
    private Consistency consistency = Consistency.EVENTUAL;


    ServerStoreConfigBuilder consistency(Consistency consistency) {
      this.consistency = consistency;
      return this;
    }

    ServerStoreConfigBuilder dedicated(String resourceName, int size, MemoryUnit unit) {
      this.poolAllocation = new Dedicated(resourceName, unit.toBytes(size));
      return this;
    }

    ServerStoreConfigBuilder shared(String resourcePoolName) {
      this.poolAllocation = new Shared(resourcePoolName);
      return this;
    }

    ServerStoreConfigBuilder unknown() {
      this.poolAllocation = new PoolAllocation.Unknown();
      return this;
    }

    ServerStoreConfigBuilder setStoredKeyType(Class<?> storedKeyType) {
      this.storedKeyType = storedKeyType.getName();
      return this;
    }

    ServerStoreConfigBuilder setStoredValueType(Class<?> storedValueType) {
      this.storedValueType = storedValueType.getName();
      return this;
    }

    ServerStoreConfigBuilder setKeySerializerType(Class<?> keySerializerType) {
      this.keySerializerType = keySerializerType.getName();
      return this;
    }

    ServerStoreConfigBuilder setValueSerializerType(Class<?> valueSerializerType) {
      this.valueSerializerType = valueSerializerType.getName();
      return this;
    }

    ServerStoreConfiguration build() {
      return new ServerStoreConfiguration(poolAllocation, storedKeyType, storedValueType,
        keySerializerType, valueSerializerType, consistency);
    }
  }

  private static final class TestClientDescriptor implements ClientDescriptor {
    private static final AtomicInteger counter = new AtomicInteger(0);

    private final int clientId = counter.incrementAndGet();

    @Override
    public String toString() {
      return "TestClientDescriptor[" + clientId + "]";
    }
  }

  /**
   * Provides a {@link ServiceRegistry} for off-heap resources.  This is a "server-side" object.
   */
  private static final class OffHeapIdentifierRegistry implements ServiceRegistry {

    private final long offHeapSize;
    private final String defaultResource;

    private EhcacheStateServiceImpl storeManagerService;

    private IEntityMessenger entityMessenger;

    private ClientCommunicator clientCommunicator;

    private final Map<OffHeapResourceIdentifier, TestOffHeapResource> pools =
        new HashMap<OffHeapResourceIdentifier, TestOffHeapResource>();

    private final Map<String, ServerSideConfiguration.Pool> sharedPools = new HashMap<>();

    /**
     * Instantiate an "open" {@code ServiceRegistry}.  Using this constructor creates a
     * registry that creates {@code OffHeapResourceIdentifier} entries as they are
     * referenced.
     */
    private OffHeapIdentifierRegistry(String defaultResource) {
      this.defaultResource = defaultResource;
      this.offHeapSize = 0;
    }

    /**
     * Instantiate a "closed" {@code ServiceRegistry}.  Using this constructor creates a
     * registry that only returns {@code OffHeapResourceIdentifier} entries supplied
     * through the {@link #addResource} method.
     */
    private OffHeapIdentifierRegistry() {
      this(null);
    }

    private void addSharedPool(String name, long size, String resourceName) {
      sharedPools.put(name, new ServerSideConfiguration.Pool(size, resourceName));
    }

    /**
     * Adds an off-heap resource of the given name to this registry.
     *
     * @param name        the name of the resource
     * @param offHeapSize the off-heap size
     * @param unit        the size unit type
     * @return {@code this} {@code OffHeapIdentifierRegistry}
     */
    private OffHeapIdentifierRegistry addResource(String name, int offHeapSize, MemoryUnit unit) {
      this.pools.put(OffHeapResourceIdentifier.identifier(name), new TestOffHeapResource(unit.toBytes(offHeapSize)));
      return this;
    }

    private TestOffHeapResource getResource(String resourceName) {
      return this.pools.get(OffHeapResourceIdentifier.identifier(resourceName));
    }

    private EhcacheStateServiceImpl getStoreManagerService() {
      return this.storeManagerService;
    }


    private IEntityMessenger getEntityMessenger() {
      return entityMessenger;
    }

    private ClientCommunicator getClientCommunicator() {
      return clientCommunicator;
    }

    private static Set<String> getIdentifiers(Set<OffHeapResourceIdentifier> pools) {
      Set<String> names = new HashSet<String>();
      for (OffHeapResourceIdentifier identifier: pools) {
        names.add(identifier.getName());
      }

      return Collections.unmodifiableSet(names);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getService(ServiceConfiguration<T> serviceConfiguration) {
      if (serviceConfiguration.getServiceType().equals(ClientCommunicator.class)) {
        if (this.clientCommunicator == null) {
          this.clientCommunicator = mock(ClientCommunicator.class);
        }
        return (T) this.clientCommunicator;
      } else if (serviceConfiguration.getServiceType().equals(EhcacheStateService.class)) {
        if (storeManagerService == null) {
          this.storeManagerService = new EhcacheStateServiceImpl(new OffHeapResources() {
            @Override
            public Set<OffHeapResourceIdentifier> getAllIdentifiers() {
              return pools.keySet();
            }

            @Override
            public OffHeapResource getOffHeapResource(OffHeapResourceIdentifier identifier) {
              return pools.get(identifier);
            }
          }, new ServerSideConfiguration(sharedPools), DEFAULT_MAPPER, service -> {});
          try {
            this.storeManagerService.configure();
          } catch (ConfigurationException e) {
            throw new AssertionError("Test setup failed!");
          }
        }
        return (T) (this.storeManagerService);
      } else if (serviceConfiguration.getServiceType().equals(IEntityMessenger.class)) {
        if (this.entityMessenger == null) {
          this.entityMessenger = mock(IEntityMessenger.class);
        }
        return (T) this.entityMessenger;
      } else if(serviceConfiguration instanceof ConsumerManagementRegistryConfiguration) {
        return null;
      } else if(serviceConfiguration instanceof ActiveEntityMonitoringServiceConfiguration) {
        return null;
      }

      throw new UnsupportedOperationException("Registry.getService does not support " + serviceConfiguration.getClass().getName());
    }
  }

  /**
   * Testing implementation of {@link OffHeapResource}.  This is a "server-side" object.
   */
  private static final class TestOffHeapResource implements OffHeapResource {

    private long capacity;
    private long used;

    private TestOffHeapResource(long capacity) {
      this.capacity = capacity;
    }

    @Override
    public boolean reserve(long size) throws IllegalArgumentException {
      if (size < 0) {
        throw new IllegalArgumentException();
      }
      if (size > available()) {
        return false;
      } else {
        this.used += size;
        return true;
      }
    }

    @Override
    public void release(long size) throws IllegalArgumentException {
      if (size < 0) {
        throw new IllegalArgumentException();
      }
      this.used -= size;
    }

    @Override
    public long available() {
      return this.capacity - this.used;
    }

    @Override
    public long capacity() {
      return capacity;
    }

    private long getUsed() {
      return used;
    }
  }

  private static class InvalidMessage extends EhcacheEntityMessage {
    @Override
    public void setId(long id) {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public long getId() {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public UUID getClientId() {
      throw new UnsupportedOperationException("TODO Implement me!");
    }
  }
}
