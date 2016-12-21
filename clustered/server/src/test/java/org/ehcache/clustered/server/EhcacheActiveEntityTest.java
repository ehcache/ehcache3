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
package org.ehcache.clustered.server;

import org.ehcache.clustered.common.internal.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerSideConfigurationException;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerStoreConfigurationException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreManagerException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.exceptions.ResourceBusyException;
import org.ehcache.clustered.common.internal.exceptions.ResourceConfigurationException;
import org.ehcache.clustered.common.internal.exceptions.ServerMisconfigurationException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Failure;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageFactory;
import org.ehcache.clustered.common.internal.messages.ServerStoreMessageFactory;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResourceIdentifier;
import org.terracotta.offheapresource.OffHeapResources;
import org.terracotta.offheapstore.util.MemoryUnit;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.ehcache.clustered.common.PoolAllocation.Dedicated;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Type;

import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Type.FAILURE;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import org.junit.Assert;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * @author cdennis
 */
public class EhcacheActiveEntityTest {

  private static final byte[] ENTITY_ID = ClusteredEhcacheIdentity.serialize(UUID.randomUUID());
  private static final LifeCycleMessageFactory MESSAGE_FACTORY = new LifeCycleMessageFactory();

  @Test
  public void testConfigTooShort() {
    try {
      new EhcacheActiveEntity(null, new byte[ENTITY_ID.length - 1]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testConfigTooLong() {
    try {
      new EhcacheActiveEntity(null, new byte[ENTITY_ID.length + 1]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testConfigNull() {
    try {
      new EhcacheActiveEntity(null, null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  /**
   * Ensures that a client connection is properly tracked.
   */
  @Test
  public void testConnected() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(new OffHeapIdentifierRegistry(), ENTITY_ID);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    final Map<ClientDescriptor, Set<String>> connectedClients = activeEntity.getConnectedClients();
    assertThat(connectedClients.keySet(), is(equalTo(Collections.singleton(client))));
    assertThat(connectedClients.get(client), is(Matchers.<String>empty()));

    assertThat(activeEntity.getInUseStores().isEmpty(), is(true));
  }

  @Test
  public void testConnectedAgain() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(new OffHeapIdentifierRegistry(), ENTITY_ID);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), is(equalTo(Collections.singleton(client))));

    activeEntity.connected(client);

    final Map<ClientDescriptor, Set<String>> connectedClients = activeEntity.getConnectedClients();
    assertThat(connectedClients.keySet(), is(equalTo(Collections.singleton(client))));
    assertThat(connectedClients.get(client), is(Matchers.<String>empty()));

    assertThat(activeEntity.getInUseStores().isEmpty(), is(true));
  }

  @Test
  public void testConnectedSecond() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(new OffHeapIdentifierRegistry(), ENTITY_ID);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);

    final Map<ClientDescriptor, Set<String>> connectedClients = activeEntity.getConnectedClients();
    assertThat(connectedClients.keySet(), containsInAnyOrder(client, client2));
    assertThat(connectedClients.get(client), is(Matchers.<String>empty()));
    assertThat(connectedClients.get(client2), is(Matchers.<String>empty()));

    assertThat(activeEntity.getInUseStores().isEmpty(), is(true));
  }

  /**
   * Ensures a disconnect for a non-connected client does not throw.
   */
  @Test
  public void testDisconnectedNotConnected() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(new OffHeapIdentifierRegistry(), ENTITY_ID);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.disconnected(client);
    // Not expected to fail ...
  }

  /**
   * Ensures the disconnect of a connected client is properly tracked.
   */
  @Test
  public void testDisconnected() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(new OffHeapIdentifierRegistry(), ENTITY_ID);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    activeEntity.disconnected(client);

    assertThat(activeEntity.getConnectedClients().isEmpty(), is(true));
    assertThat(activeEntity.getInUseStores().isEmpty(), is(true));
  }

  /**
   * Ensures the disconnect of a connected client is properly tracked and does not affect others.
   */
  @Test
  public void testDisconnectedSecond() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(new OffHeapIdentifierRegistry(), ENTITY_ID);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client, client2));

    activeEntity.disconnected(client);

    assertThat(activeEntity.getConnectedClients().keySet(), is(equalTo(Collections.singleton(client2))));
    assertThat(activeEntity.getInUseStores().isEmpty(), is(true));
  }

  /**
   * Ensures basic shared resource pool configuration.
   */
  @Test
  public void testConfigure() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .defaultResource("defaultServerResource")
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testNoAttachementFailsToInvokeServerStoreOperation() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(
        activeEntity.invoke(client,
            MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
                .defaultResource("defaultServerResource")
                .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
                .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
                .build()))
    );

    assertSuccess(activeEntity.invoke(client,
            MESSAGE_FACTORY.createServerStore("testNoAttachementFailsToInvokeServerStoreOperation", new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build()))
    );

    // disconnect the client and connect a new non-attached one
    activeEntity.disconnected(client);
    client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory("testNoAttachementFailsToInvokeServerStoreOperation");

    assertFailure(
        activeEntity.invoke(client, messageFactory.appendOperation(1L, createPayload(1L))),
        LifecycleException.class, "not attached"
    );
  }

  @Test
  public void testAppendInvalidationAcksTakenIntoAccount() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client1 = new TestClientDescriptor();
    ClientDescriptor client2 = new TestClientDescriptor();
    ClientDescriptor client3 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.connected(client2);
    activeEntity.connected(client3);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertSuccess(
        activeEntity.invoke(client1,
            MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration))
    );

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .consistency(Consistency.STRONG)
        .build();
    assertSuccess(
        activeEntity.invoke(client1,
            MESSAGE_FACTORY.createServerStore("testDisconnection", serverStoreConfiguration))
    );

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory("testDisconnection");

    // attach the clients
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );

    // attach to the store
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );

    // perform an append
    assertSuccess(
        activeEntity.invoke(client1, messageFactory.appendOperation(1L, createPayload(1L)))
    );

    // assert that an invalidation request is pending
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    EhcacheActiveEntity.InvalidationHolder invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(2));
    assertThat(invalidationHolder.clientsHavingToInvalidate, containsInAnyOrder(client2, client3));

    // client 2 acks
    assertSuccess(
        activeEntity.invoke(client2, messageFactory.clientInvalidationAck(activeEntity.getClientsWaitingForInvalidation().keySet().iterator().next()))
    );

    // assert that client 2 is not waited for anymore
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(1));
    assertThat(invalidationHolder.clientsHavingToInvalidate, contains(client3));

    // client 3 acks
    assertSuccess(
        activeEntity.invoke(client3, messageFactory.clientInvalidationAck(activeEntity.getClientsWaitingForInvalidation().keySet().iterator().next()))
    );

    // assert that the invalidation request is done since all clients disconnected
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(0));
  }

  @Test
  public void testClearInvalidationAcksTakenIntoAccount() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client1 = new TestClientDescriptor();
    ClientDescriptor client2 = new TestClientDescriptor();
    ClientDescriptor client3 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.connected(client2);
    activeEntity.connected(client3);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertSuccess(
        activeEntity.invoke(client1,
            MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration))
    );

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .consistency(Consistency.STRONG)
        .build();
    assertSuccess(
        activeEntity.invoke(client1,
            MESSAGE_FACTORY.createServerStore("testDisconnection", serverStoreConfiguration))
    );

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory("testDisconnection");

    // attach the clients
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );

    // attach to the store
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );

    // perform a clear
    assertSuccess(
        activeEntity.invoke(client1, messageFactory.clearOperation())
    );

    // assert that an invalidation request is pending
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    EhcacheActiveEntity.InvalidationHolder invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(2));
    assertThat(invalidationHolder.clientsHavingToInvalidate, containsInAnyOrder(client2, client3));

    // client 2 acks
    assertSuccess(
        activeEntity.invoke(client2, messageFactory.clientInvalidationAck(activeEntity.getClientsWaitingForInvalidation().keySet().iterator().next()))
    );

    // assert that client 2 is not waited for anymore
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(1));
    assertThat(invalidationHolder.clientsHavingToInvalidate, contains(client3));

    // client 3 acks
    assertSuccess(
        activeEntity.invoke(client3, messageFactory.clientInvalidationAck(activeEntity.getClientsWaitingForInvalidation().keySet().iterator().next()))
    );

    // assert that the invalidation request is done since all clients disconnected
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(0));
  }

  @Test
  public void testAppendInvalidationDisconnectionOfInvalidatingClientsTakenIntoAccount() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client1 = new TestClientDescriptor();
    ClientDescriptor client2 = new TestClientDescriptor();
    ClientDescriptor client3 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.connected(client2);
    activeEntity.connected(client3);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertSuccess(
        activeEntity.invoke(client1,
            MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration))
    );

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .consistency(Consistency.STRONG)
        .build();
    assertSuccess(
        activeEntity.invoke(client1,
            MESSAGE_FACTORY.createServerStore("testDisconnection", serverStoreConfiguration))
    );

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory("testDisconnection");

    // attach the clients
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );

    // attach to the store
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );

    // perform an append
    assertSuccess(
        activeEntity.invoke(client1, messageFactory.appendOperation(1L, createPayload(1L)))
    );

    // assert that an invalidation request is pending
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    EhcacheActiveEntity.InvalidationHolder invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
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
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client1 = new TestClientDescriptor();
    ClientDescriptor client2 = new TestClientDescriptor();
    ClientDescriptor client3 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.connected(client2);
    activeEntity.connected(client3);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertSuccess(
        activeEntity.invoke(client1,
            MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration))
    );

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .consistency(Consistency.STRONG)
        .build();
    assertSuccess(
        activeEntity.invoke(client1,
            MESSAGE_FACTORY.createServerStore("testDisconnection", serverStoreConfiguration))
    );

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory("testDisconnection");

    // attach the clients
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );

    // attach to the store
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );

    // perform an append
    assertSuccess(
        activeEntity.invoke(client1, messageFactory.clearOperation())
    );

    // assert that an invalidation request is pending
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    EhcacheActiveEntity.InvalidationHolder invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
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
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client1 = new TestClientDescriptor();
    ClientDescriptor client2 = new TestClientDescriptor();
    ClientDescriptor client3 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.connected(client2);
    activeEntity.connected(client3);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertSuccess(
        activeEntity.invoke(client1,
            MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration))
    );

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .consistency(Consistency.STRONG)
        .build();
    assertSuccess(
        activeEntity.invoke(client1,
            MESSAGE_FACTORY.createServerStore("testDisconnection", serverStoreConfiguration))
    );

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory("testDisconnection");

    // attach the clients
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );

    // attach to the store
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );

    // perform an append
    assertSuccess(
        activeEntity.invoke(client1, messageFactory.appendOperation(1L, createPayload(1L)))
    );

    // assert that an invalidation request is pending
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    EhcacheActiveEntity.InvalidationHolder invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
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
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client1 = new TestClientDescriptor();
    ClientDescriptor client2 = new TestClientDescriptor();
    ClientDescriptor client3 = new TestClientDescriptor();
    activeEntity.connected(client1);
    activeEntity.connected(client2);
    activeEntity.connected(client3);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertSuccess(
        activeEntity.invoke(client1,
            MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration))
    );

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .consistency(Consistency.STRONG)
        .build();
    assertSuccess(
        activeEntity.invoke(client1,
            MESSAGE_FACTORY.createServerStore("testDisconnection", serverStoreConfiguration))
    );

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory("testDisconnection");

    // attach the clients
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );

    // attach to the store
    assertSuccess(
        activeEntity.invoke(client1, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );
    assertSuccess(
        activeEntity.invoke(client3, MESSAGE_FACTORY.validateServerStore("testDisconnection", serverStoreConfiguration))
    );

    // perform an append
    assertSuccess(
        activeEntity.invoke(client1, messageFactory.clearOperation())
    );

    // assert that an invalidation request is pending
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    EhcacheActiveEntity.InvalidationHolder invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(2));
    assertThat(invalidationHolder.clientsHavingToInvalidate, containsInAnyOrder(client2, client3));

    // disconnect client1
    activeEntity.disconnected(client1);

    // assert that the invalidation request is done since the originating client disconnected
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(0));
  }

  @Test
  public void testAttachedClientButNotStoreFailsInvokingServerStoreOperation() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertSuccess(
        activeEntity.invoke(client,
            MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration))
    );

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .build();
    assertSuccess(
        activeEntity.invoke(client,
            MESSAGE_FACTORY.createServerStore("testAttachedClientButNotStoreFailsInvokingServerStoreOperation", serverStoreConfiguration))
    );

    // disconnect the client and connect a new non-attached one
    activeEntity.disconnected(client);
    client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory("testAttachedClientButNotStoreFailsInvokingServerStoreOperation");

    // attach the client
    assertSuccess(
        activeEntity.invoke(client, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );

    assertFailure(
        activeEntity.invoke(client, messageFactory.appendOperation(1L, createPayload(1L))),
        LifecycleException.class, "Client not attached to clustered tier 'testAttachedClientButNotStoreFailsInvokingServerStoreOperation'"
    );
  }

  @Test
  public void testWithAttachmentSucceedsInvokingServerStoreOperation() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertSuccess(
        activeEntity.invoke(client,
            MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration))
    );

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .build();
    assertSuccess(
        activeEntity.invoke(client,
            MESSAGE_FACTORY.createServerStore("testWithAttachmentSucceedsInvokingServerStoreOperation", serverStoreConfiguration))
    );

    // disconnect the client and connect a new non-attached one
    activeEntity.disconnected(client);
    client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    ServerStoreMessageFactory messageFactory = new ServerStoreMessageFactory("testWithAttachmentSucceedsInvokingServerStoreOperation");

    // attach the client
    assertSuccess(
        activeEntity.invoke(client, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration))
    );

    // attach to the store
    assertSuccess(
        activeEntity.invoke(client, MESSAGE_FACTORY.validateServerStore("testWithAttachmentSucceedsInvokingServerStoreOperation", serverStoreConfiguration))
    );

    assertSuccess(
        activeEntity.invoke(client, messageFactory.appendOperation(1L, createPayload(1L)))
    );
  }

  @Test
  public void testConfigureBeforeConnect() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .defaultResource("defaultServerResource")
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())),
        LifecycleException.class, "not connected");
  }

  @Test
  public void testConfigureAfterConfigure() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .defaultResource("defaultServerResource")
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    activeEntity.disconnected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), is(Matchers.<ClientDescriptor>empty()));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client2));

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .defaultResource("defaultServerResource")
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())),
        InvalidStoreManagerException.class, "already configured");

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

    assertThat(activeEntity.getConnectedClients().get(client2), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  /**
   * Ensure configuration fails when specifying non-existent resource.
   */
  @Test
  public void testConfigureMissingPoolResource() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 64, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .defaultResource("defaultServerResource")
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)    // missing on 'server'
            .build())),
        ResourceConfigurationException.class, "Non-existent server side resource");

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(0L));
    assertThat(registry.getResource("serverResource2"), is(nullValue()));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  /**
   * Ensure configuration fails when specifying non-existent default resource.
   */
  @Test
  public void testConfigureMissingDefaultResource() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .defaultResource("defaultServerResource")
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())),
        ResourceConfigurationException.class, "Default server resource");

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(0L));
    assertThat(registry.getResource("serverResource2").getUsed(), is(0L));
    assertThat(registry.getResource("defaultServerResource"), is(nullValue()));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testConfigureLargeSharedPool() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource", 64, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .defaultResource("defaultServerResource")
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .sharedPool("tooBig", "serverResource2", 64, MemoryUnit.MEGABYTES)
            .build())),
        ResourceConfigurationException.class, "Insufficient defined resources");

    final Set<String> poolIds = registry.getStoreManagerService().getSharedResourcePoolIds();
    assertThat(poolIds, is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(0L));
    assertThat(registry.getResource("serverResource2").getUsed(), is(0L));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidate2Clients() throws Exception {
    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfig)));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfig)));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().get(client2), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidate1Client() throws Exception {
    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfig)));

    activeEntity.disconnected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), is(Matchers.<ClientDescriptor>empty()));

    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.validateStoreManager(serverSideConfig)));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidateAfterConfigure() throws Exception {
    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));
    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfig)));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.validateStoreManager(serverSideConfig)));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidateExtraResource() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .defaultResource("defaultServerResource")
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.validateStoreManager(new ServerSideConfigBuilder()
            .defaultResource("defaultServerResource")
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .sharedPool("tertiary", "serverResource3", 8, MemoryUnit.MEGABYTES)   // extra
            .build())),
        InvalidServerSideConfigurationException.class, "Pool names not equal.");
  }

  @Test
  public void testValidateNoDefaultResource() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.validateStoreManager(new ServerSideConfigBuilder()
            .defaultResource("defaultServerResource")
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())),
        InvalidServerSideConfigurationException.class, "Default resource not aligned");
  }

  @Test
  public void testCreateDedicatedServerStoreBeforeConfigure() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())),
        LifecycleException.class, "not attached");
  }

  @Test
  public void testCreateDedicatedServerStoreBeforeValidate() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    activeEntity.disconnected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), is(Matchers.<ClientDescriptor>empty()));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client2));

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())),
        LifecycleException.class, "not attached");
  }

  @Test
  public void testCreateDedicatedServerStore() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), contains(client));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    /*
     * Ensure the dedicated resource pool remains after client disconnect.
     */
    activeEntity.disconnected(client);

    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(activeEntity.getConnectedClients().get(client), is(nullValue()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testCreateDedicatedServerStoreAfterValidate() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfig)));

    activeEntity.disconnected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), is(Matchers.<ClientDescriptor>empty()));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfig)));

    assertSuccess(activeEntity.invoke(client2,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client2), contains("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), contains(client2));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testCreateDedicatedServerStoreExisting() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));
    activeEntity.disconnected(client);
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())),
        InvalidStoreException.class, "already exists");
  }

  @Test
  public void testCreateReleaseDedicatedServerStoreMultiple() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("secondCache",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource2", 8, MemoryUnit.MEGABYTES)
                .build())));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias", "secondCache"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L + 8L)));

    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("cacheAlias", "secondCache"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), contains(client));
    assertThat(activeEntity.getInUseStores().get("secondCache"), contains(client));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias", "secondCache"));

    /*
     * Separately release the stores ...
     */
    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("cacheAlias")));

    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias", "secondCache"));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("secondCache"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(activeEntity.getInUseStores().get("secondCache"), contains(client));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.releaseServerStore("cacheAlias")),
        InvalidStoreException.class, "not in use");

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("secondCache")));

    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias", "secondCache"));
    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(activeEntity.getInUseStores().get("secondCache"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias", "secondCache"));
  }

  @Test
  public void testValidateDedicatedServerStore() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);
    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias", serverStoreConfiguration)));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client2,
        MESSAGE_FACTORY.validateServerStore("cacheAlias", serverStoreConfiguration)));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().get(client2), contains("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), containsInAnyOrder(client, client2));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testValidateDedicatedServerStoreBad() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);
    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.validateServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 8, MemoryUnit.MEGABYTES)
                .build())),
        InvalidServerStoreConfigurationException.class);

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().get(client2), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), containsInAnyOrder(client));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testValidateDedicatedServerStoreBeforeCreate() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfig)));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfig)));

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.validateServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())),
        InvalidStoreException.class, "does not exist");

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().get(client2), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), is(nullValue()));
    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testCreateSharedServerStore() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build())));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), contains(client));

    /*
     * Ensure the shared resource store remains after client disconnect.
     */
    activeEntity.disconnected(client);

    assertThat(activeEntity.getConnectedClients().get(client), is(nullValue()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testCreateSharedServerStoreExisting() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);
    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build())));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    activeEntity.disconnected(client);

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client2));

    assertSuccess(activeEntity.invoke(client2,
        MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build())),
        InvalidStoreException.class, "already exists");
  }

  @Test
  public void testValidateSharedServerStore() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);
    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ServerStoreConfiguration sharedStoreConfig = new ServerStoreConfigBuilder()
        .shared("primary")
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias", sharedStoreConfig)));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), contains(client));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client2,
        MESSAGE_FACTORY.validateServerStore("cacheAlias", sharedStoreConfig)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().get(client2), contains("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), containsInAnyOrder(client, client2));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testValidateServerStore_DedicatedStoresDifferentSizes() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 4, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder().build();

    ServerStoreConfiguration dedicatedStoreConfig1 = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .build();

    ServerStoreConfiguration dedicatedStoreConfig2 = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 2, MemoryUnit.MEGABYTES)
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client1 = new TestClientDescriptor();
    activeEntity.connected(client1);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client1));

    assertSuccess(activeEntity.invoke(client1, MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client1, MESSAGE_FACTORY.createServerStore("cacheAlias", dedicatedStoreConfig1)));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);

    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client1, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    String expectedMessageContent = "Existing ServerStore configuration is not compatible with the desired configuration: " +
                                    "\n\t" +
                                    "resourcePoolDedicatedSize existing: " +
                                    ((PoolAllocation.Dedicated)dedicatedStoreConfig1.getPoolAllocation()).getSize() +
                                    ", desired: " +
                                    ((PoolAllocation.Dedicated)dedicatedStoreConfig2.getPoolAllocation()).getSize();

    assertFailure(activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore("cacheAlias", dedicatedStoreConfig2)),InvalidServerStoreConfigurationException.class,expectedMessageContent);
    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
  }

  @Test
  public void testValidateServerStore_DedicatedStoresSameSizes() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 4, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder().build();

    ServerStoreConfiguration dedicatedStoreConfig1 = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .build();

    ServerStoreConfiguration dedicatedStoreConfig2 = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client1 = new TestClientDescriptor();
    activeEntity.connected(client1);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client1));

    assertSuccess(activeEntity.invoke(client1, MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client1, MESSAGE_FACTORY.createServerStore("cacheAlias", dedicatedStoreConfig1)));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);

    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client1, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));
    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore("cacheAlias", dedicatedStoreConfig2)));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
  }

  @Test
  public void testValidateServerStore_DedicatedStoreResourceNamesDifferent() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 4, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder().build();

    ServerStoreConfiguration dedicatedStoreConfig1 = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .build();

    ServerStoreConfiguration dedicatedStoreConfig2 = new ServerStoreConfigBuilder()
        .dedicated("serverResource2", 4, MemoryUnit.MEGABYTES)
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client1 = new TestClientDescriptor();
    activeEntity.connected(client1);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client1));

    assertSuccess(activeEntity.invoke(client1, MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client1, MESSAGE_FACTORY.createServerStore("cacheAlias", dedicatedStoreConfig1)));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);

    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client1, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));
    String expectedMessageContent = "Existing ServerStore configuration is not compatible with the desired configuration: " +
                                    "\n\t" +
                                    "resourcePoolDedicatedResourceName existing: " +
                                    ((Dedicated)dedicatedStoreConfig1.getPoolAllocation()).getResourceName() +
                                    ", desired: " +
                                    ((Dedicated)dedicatedStoreConfig2.getPoolAllocation()).getResourceName();

    assertFailure(activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore("cacheAlias", dedicatedStoreConfig2)),InvalidServerStoreConfigurationException.class,expectedMessageContent);
    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
  }

  @Test
  public void testValidateServerStore_DedicatedCacheNameDifferent() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 4, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder().build();

    ServerStoreConfiguration dedicatedStoreConfig1 = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .build();

    ServerStoreConfiguration dedicatedStoreConfig2 = new ServerStoreConfigBuilder()
        .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client1 = new TestClientDescriptor();
    activeEntity.connected(client1);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client1));

    assertSuccess(activeEntity.invoke(client1, MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client1, MESSAGE_FACTORY.createServerStore("cacheAlias", dedicatedStoreConfig1)));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);

    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client1, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));
    assertFailure(activeEntity.invoke(client2, MESSAGE_FACTORY.validateServerStore("cacheAliasBad", dedicatedStoreConfig2)),InvalidStoreException.class,"Clustered tier 'cacheAliasBad' does not exist");

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
  }

  @Test
  public void testServerStoreSameNameInDifferentSharedPools() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);
    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build())));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    activeEntity.disconnected(client);

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client2));

    assertSuccess(activeEntity.invoke(client2,
        MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("secondary")
                .build())),InvalidStoreException.class,"Clustered tier 'cacheAlias' already exists");
  }


  @Test
  public void testValidateSharedServerStoreBad() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);
    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build())));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.validateServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("secondary")
                .build())),
        InvalidServerStoreConfigurationException.class);

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().get(client2), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), contains(client));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testReleaseServerStoreBeforeAttach() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.releaseServerStore("cacheAlias")),
        InvalidStoreException.class, "does not exist");
  }

  @Test
  public void testReleaseServerStoreAfterRelease() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), containsInAnyOrder(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("cacheAlias")));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), is(Matchers.<ClientDescriptor>empty()));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.releaseServerStore("cacheAlias")),
        InvalidStoreException.class, "not in use by client");
  }

  @Test
  public void testDestroyServerStore() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("dedicatedCache",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache"));
    assertThat(activeEntity.getInUseStores().get("dedicatedCache"), containsInAnyOrder(client));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("dedicatedCache")));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache"));
    assertThat(activeEntity.getInUseStores().get("dedicatedCache"), is(Matchers.<ClientDescriptor>empty()));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("sharedCache",
            new ServerStoreConfigBuilder()
                .shared("secondary")
                .build())));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder("dedicatedCache", "sharedCache"));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("sharedCache")));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache"));
    assertThat(activeEntity.getInUseStores().get("dedicatedCache"), is(Matchers.<ClientDescriptor>empty()));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.destroyServerStore("sharedCache")));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache"));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder("dedicatedCache"));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.destroyServerStore("dedicatedCache")));

    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));
  }

  /**
   * Tests the destroy server store operation <b>before</b> the use of either a
   * {@link LifecycleMessage.CreateServerStore CreateServerStore}
   * {@link LifecycleMessage.ValidateServerStore ValidateServerStore}
   * operation.
   */
  @Test
  public void testDestroyServerStoreBeforeAttach() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.destroyServerStore("cacheAlias")),
        InvalidStoreException.class, "does not exist");
  }

  @Test
  public void testDestroyServerStoreInUse() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("dedicatedCache",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("sharedCache",
            new ServerStoreConfigBuilder()
                .shared("secondary")
                .build())));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder("dedicatedCache", "sharedCache"));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.destroyServerStore("sharedCache")),
        ResourceBusyException.class, "in use");

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.destroyServerStore("dedicatedCache")),
        ResourceBusyException.class, "in use");

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(activeEntity.getInUseStores().get("dedicatedCache"), contains(client));
    assertThat(activeEntity.getInUseStores().get("sharedCache"), contains(client));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));
  }

  /**
   * Ensures shared pool and store (cache) name spaces are independent.
   * The cache alias is used as the name for a {@code ServerStore} instance; this name can be
   * the same as, but is independent of, the shared pool name.  The
   */
  @Test
  public void testSharedPoolCacheNameCollision() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource", 64, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));
    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("dedicatedCache",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("dedicatedCache"));
    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache"));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache"));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("sharedCache",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build())));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache"));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache"));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("primary",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource2", 4, MemoryUnit.MEGABYTES)
                .build())));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("dedicatedCache", "sharedCache", "primary"));
    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache", "primary"));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache", "primary"));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("dedicatedCache")));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("sharedCache", "primary"));
    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache", "primary"));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache", "primary"));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("primary")));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("sharedCache"));
    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache", "primary"));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache", "primary"));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("sharedCache")));
    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache", "primary"));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache", "primary"));
  }

  @Test
  public void testDestroyEmpty() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(new OffHeapIdentifierRegistry() , ENTITY_ID);
    activeEntity.destroy();
  }

  @Test
  public void testDestroyWithStores() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("dedicatedCache",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("sharedCache",
            new ServerStoreConfigBuilder()
                .shared("secondary")
                .build())));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder("dedicatedCache", "sharedCache"));

    activeEntity.disconnected(client);

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(activeEntity.getConnectedClients().keySet(), is(Matchers.<ClientDescriptor>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(activeEntity.getInUseStores().get("dedicatedCache"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(activeEntity.getInUseStores().get("sharedCache"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), contains("dedicatedCache"));
    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client2));

    activeEntity.destroy();

    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().get(client2), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(0L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(0L)));
  }

  @Test
  public void testValidateIdenticalConfiguration() {
    ServerSideConfiguration configureConfig = new ServerSideConfigBuilder()
        .defaultResource("primary-server-resource")
        .sharedPool("foo", null, 8, MemoryUnit.MEGABYTES)
        .sharedPool("bar", "secondary-server-resource", 8, MemoryUnit.MEGABYTES)
        .build();

    ServerSideConfiguration validateConfig = new ServerSideConfigBuilder()
        .defaultResource("primary-server-resource")
        .sharedPool("foo", null, 8, MemoryUnit.MEGABYTES)
        .sharedPool("bar", "secondary-server-resource", 8, MemoryUnit.MEGABYTES)
        .build();

    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("primary-server-resource", 16, MemoryUnit.MEGABYTES);
    registry.addResource("secondary-server-resource", 16, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor configurer = new TestClientDescriptor();
    activeEntity.connected(configurer);
    activeEntity.invoke(configurer, MESSAGE_FACTORY.configureStoreManager(configureConfig));

    ClientDescriptor validator = new TestClientDescriptor();
    activeEntity.connected(validator);

    assertThat(activeEntity.invoke(validator, MESSAGE_FACTORY.validateStoreManager(validateConfig)).getType(), is(Type.SUCCESS));
  }

  @Test
  public void testValidateSharedPoolNamesDifferent() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 64, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);

    ClientDescriptor configurer = new TestClientDescriptor();
    activeEntity.connected(configurer);
    ServerSideConfiguration configure = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    activeEntity.invoke(configurer, MESSAGE_FACTORY.configureStoreManager(configure));

    ClientDescriptor validator = new TestClientDescriptor();
    activeEntity.connected(validator);
    ServerSideConfiguration validate = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("ternary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertFailure(activeEntity.invoke(validator, MESSAGE_FACTORY.validateStoreManager(validate)), InvalidServerSideConfigurationException.class, "Pool names not equal.");
  }

  @Test
  public void testValidateDefaultResourceNameDifferent() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);

    ClientDescriptor configurer = new TestClientDescriptor();
    activeEntity.connected(configurer);
    ServerSideConfiguration configure = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource1")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertSuccess(activeEntity.invoke(configurer,MESSAGE_FACTORY.configureStoreManager(configure)));

    ClientDescriptor validator = new TestClientDescriptor();
    activeEntity.connected(validator);
    ServerSideConfiguration validate = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource2")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertFailure(activeEntity.invoke(validator, MESSAGE_FACTORY.validateStoreManager(validate)), InvalidServerSideConfigurationException.class, "Default resource not aligned.");
  }

  @Test
  public void testValidateClientSharedPoolSizeTooBig() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(64, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);

    ClientDescriptor configurer = new TestClientDescriptor();
    activeEntity.connected(configurer);
    ServerSideConfiguration configure = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource1")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 32, MemoryUnit.MEGABYTES)
        .build();
    activeEntity.invoke(configurer,MESSAGE_FACTORY.configureStoreManager(configure));

    ClientDescriptor validator = new TestClientDescriptor();
    activeEntity.connected(validator);
    ServerSideConfiguration validate = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource1")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 36, MemoryUnit.MEGABYTES)
        .build();
    assertFailure(activeEntity.invoke(validator, MESSAGE_FACTORY.validateStoreManager(validate)),InvalidServerSideConfigurationException.class, "Pool 'secondary' not equal.");
  }

  @Test
  public void testValidateSecondClientInheritsFirstClientConfig() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);

    ClientDescriptor configurer = new TestClientDescriptor();
    activeEntity.connected(configurer);
    ServerSideConfiguration initialConfiguration = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    activeEntity.invoke(configurer, MESSAGE_FACTORY.configureStoreManager(initialConfiguration));

    ClientDescriptor validator = new TestClientDescriptor();
    activeEntity.connected(validator);
    assertSuccess(activeEntity.invoke(validator, MESSAGE_FACTORY.validateStoreManager(null)));
  }

  @Test
  public void testValidateNonExistentSharedPool() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);

    EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);

    ClientDescriptor configurer = new TestClientDescriptor();
    activeEntity.connected(configurer);
    ServerSideConfiguration configure = new ServerSideConfigBuilder().build();
    assertSuccess(activeEntity.invoke(configurer,MESSAGE_FACTORY.configureStoreManager(configure)));

    ServerStoreConfiguration sharedStoreConfig = new ServerStoreConfigBuilder()
        .shared("non-existent-pool")
        .build();

    assertFailure(
        activeEntity.invoke(configurer, MESSAGE_FACTORY.createServerStore("sharedCache", sharedStoreConfig)),
        ResourceConfigurationException.class,
        "Shared pool named 'non-existent-pool' undefined."
    );
  }

  @Test
  public void testCreateServerStoreWithUnknownPool() throws Exception {

    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .build();
    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .unknown()
        .build();

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration)));

    try {
      assertSuccess(activeEntity.invoke(client,
          MESSAGE_FACTORY.createServerStore("cacheAlias", serverStoreConfiguration)));
      fail("Expecting LifecycleException");
    } catch(Exception e) {
      assertThat(e, instanceOf(LifecycleException.class));
      assertThat(e.getMessage(), is("Clustered tier can't be created with an Unknown resource pool"));
    }
  }

  private void assertSuccess(EhcacheEntityResponse response) throws Exception {
    if (!response.equals(EhcacheEntityResponse.Success.INSTANCE)) {
      throw ((Failure) response).getCause();
    }
  }

  private void assertFailure(EhcacheEntityResponse response, Class<? extends Exception> expectedException) {
    assertThat(response.getType(), is(FAILURE));
    assertThat(((Failure) response).getCause(), is(instanceOf(expectedException)));
  }

  private void assertFailure(EhcacheEntityResponse response, Class<? extends Exception> expectedException, String expectedMessageContent) {
    assertThat(response.getType(), is(FAILURE));
    Exception cause = ((Failure) response).getCause();
    assertThat(cause, is(instanceOf(expectedException)));
    assertThat(cause.getMessage(), containsString(expectedMessageContent));
  }

  private static Pool pool(String resourceName, int poolSize, MemoryUnit unit) {
    return new Pool(unit.toBytes(poolSize), resourceName);
  }

  private static final class ServerSideConfigBuilder {
    private final Map<String, Pool> pools = new HashMap<String, Pool>();
    private String defaultServerResource;

    ServerSideConfigBuilder sharedPool(String poolName, String resourceName, int size, MemoryUnit unit) {
      pools.put(poolName, pool(resourceName, size, unit));
      return this;
    }

    ServerSideConfigBuilder defaultResource(String resourceName) {
      this.defaultServerResource = resourceName;
      return this;
    }

    ServerSideConfiguration build() {
      if (defaultServerResource == null) {
        return new ServerSideConfiguration(pools);
      } else {
        return new ServerSideConfiguration(defaultServerResource, pools);
      }
    }
  }

  /**
   * Builder for {@link ServerStoreConfiguration} instances.
   */
  private static final class ServerStoreConfigBuilder {
    private PoolAllocation poolAllocation;
    private String storedKeyType;
    private String storedValueType;
    private String actualKeyType;
    private String actualValueType;
    private String keySerializerType;
    private String valueSerializerType;
    private Consistency consistency;


    ServerStoreConfigBuilder consistency(Consistency consistency) {
      this.consistency = consistency;
      return this;
    }

    ServerStoreConfigBuilder dedicated(String resourceName, int size, MemoryUnit unit) {
      this.poolAllocation = new PoolAllocation.Dedicated(resourceName, unit.toBytes(size));
      return this;
    }

    ServerStoreConfigBuilder shared(String resourcePoolName) {
      this.poolAllocation = new PoolAllocation.Shared(resourcePoolName);
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

    ServerStoreConfigBuilder setActualKeyType(Class<?> actualKeyType) {
      this.actualKeyType = actualKeyType.getName();
      return this;
    }

    ServerStoreConfigBuilder setActualValueType(Class<?> actualValueType) {
      this.actualValueType = actualValueType.getName();
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
          actualKeyType, actualValueType, keySerializerType, valueSerializerType, consistency);
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

    private EhcacheStateServiceImpl storeManagerService;
    private ClientCommunicator clientCommunicator;

    private final Map<OffHeapResourceIdentifier, TestOffHeapResource> pools =
        new HashMap<OffHeapResourceIdentifier, TestOffHeapResource>();

    /**
     * Instantiate an "open" {@code ServiceRegistry}.  Using this constructor creates a
     * registry that creates {@code OffHeapResourceIdentifier} entries as they are
     * referenced.
     */
    private OffHeapIdentifierRegistry(int offHeapSize, MemoryUnit unit) {
      this.offHeapSize = unit.toBytes(offHeapSize);
    }

    /**
     * Instantiate a "closed" {@code ServiceRegistry}.  Using this constructor creates a
     * registry that only returns {@code OffHeapResourceIdentifier} entries supplied
     * through the {@link #addResource} method.
     */
    private OffHeapIdentifierRegistry() {
      this.offHeapSize = 0;
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
          });
        }
        return (T) (this.storeManagerService);
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
      return this.capacity;
    }

    private long getUsed() {
      return used;
    }
  }
}
