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

import org.ehcache.clustered.common.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.ClusteredStoreValidationException;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.ServerStoreConfiguration.PoolAllocation;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse.Failure;
import org.ehcache.clustered.common.messages.LifeCycleMessageFactory;
import org.ehcache.clustered.common.messages.ServerStoreMessageFactory;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResourceIdentifier;
import org.terracotta.offheapstore.util.MemoryUnit;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.clustered.common.messages.EhcacheEntityResponse.Type.FAILURE;
import static org.ehcache.clustered.common.store.Util.createPayload;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
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
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(mock(ServiceRegistry.class), ENTITY_ID);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    final Map<ClientDescriptor, Set<String>> connectedClients = activeEntity.getConnectedClients();
    assertThat(connectedClients.keySet(), is(equalTo(Collections.singleton(client))));
    assertThat(connectedClients.get(client), is(Matchers.<String>empty()));

    assertThat(activeEntity.getInUseStores().isEmpty(), is(true));
  }

  @Test
  public void testConnectedAgain() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(mock(ServiceRegistry.class), ENTITY_ID);

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
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(mock(ServiceRegistry.class), ENTITY_ID);

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
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(mock(ServiceRegistry.class), ENTITY_ID);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.disconnected(client);
    // Not expected to fail ...
  }

  /**
   * Ensures the disconnect of a connected client is properly tracked.
   */
  @Test
  public void testDisconnected() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(mock(ServiceRegistry.class), ENTITY_ID);

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
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(mock(ServiceRegistry.class), ENTITY_ID);

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

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testNoAttachementFailsToInvokeServerStoreOperation() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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

    assertSuccess(
        activeEntity.invoke(client,
            MESSAGE_FACTORY.createServerStore("testNoAttachementFailsToInvokeServerStoreOperation", new ServerStoreConfigBuilder()
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
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
        IllegalStateException.class, "Client not attached"
    );
  }

  @Test
  public void testInvalidationAcksTakenIntoAccount() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
        .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
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
        activeEntity.invoke(client2, messageFactory.clientInvalidateHashAck(1L, activeEntity.getClientsWaitingForInvalidation().keySet().iterator().next()))
    );

    // assert that client 2 is not waited for anymore
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(1));
    invalidationHolder = activeEntity.getClientsWaitingForInvalidation().values().iterator().next();
    assertThat(invalidationHolder.clientDescriptorWaitingForInvalidation, is(client1));
    assertThat(invalidationHolder.clientsHavingToInvalidate.size(), is(1));
    assertThat(invalidationHolder.clientsHavingToInvalidate, contains(client3));

    // client 3 acks
    assertSuccess(
        activeEntity.invoke(client3, messageFactory.clientInvalidateHashAck(1L, activeEntity.getClientsWaitingForInvalidation().keySet().iterator().next()))
    );

    // assert that the invalidation request is done since all clients disconnected
    assertThat(activeEntity.getClientsWaitingForInvalidation().size(), is(0));
  }

  @Test
  public void testInvalidationDisconnectionOfInvalidatingClientsTakenIntoAccount() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
        .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
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
  public void testInvalidationDisconnectionOfBlockingClientTakenIntoAccount() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
        .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
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
  public void testAttachedClientButNotStoreFailsInvokingServerStoreOperation() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
        .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
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
        IllegalStateException.class, "Client not attached on Server Store for cacheId : testAttachedClientButNotStoreFailsInvokingServerStoreOperation"
    );
  }

  @Test
  public void testWithAttachmentSucceedsInvokingServerStoreOperation() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
        .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
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

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .defaultResource("defaultServerResource")
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())),
        IllegalStateException.class, "not connected");
  }

  @Test
  public void testConfigureAfterConfigure() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
        IllegalStateException.class, "already configured");

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

    assertThat(activeEntity.getConnectedClients().get(client2), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
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
        IllegalArgumentException.class, "Non-existent server side resource");

    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(0L));
    assertThat(registry.getResource("serverResource2"), is(nullValue()));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
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
        IllegalArgumentException.class, "Default server resource");

    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(0L));
    assertThat(registry.getResource("serverResource2").getUsed(), is(0L));
    assertThat(registry.getResource("defaultServerResource"), is(nullValue()));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
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
        IllegalArgumentException.class, "Insufficient defined resources");

    final Set<String> poolIds = activeEntity.getSharedResourcePoolIds();
    assertThat(poolIds, is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(0L));
    assertThat(registry.getResource("serverResource2").getUsed(), is(0L));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidate2Clients() throws Exception {
    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidate1Client() throws Exception {
    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidateAfterConfigure() throws Exception {
    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));
    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfig)));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.validateStoreManager(serverSideConfig)));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidateExtraResource() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
        IllegalArgumentException.class, "SharedPoolResources aren't valid.");
  }

  @Test
  public void testValidateNoDefaultResource() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
        IllegalArgumentException.class, "Default resource not aligned");
  }

  @Test
  public void testCreateFixedServerStoreBeforeConfigure() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())),
        IllegalStateException.class, "not attached");
  }

  @Test
  public void testCreateFixedServerStoreBeforeValidate() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())),
        IllegalStateException.class, "not attached");
  }

  @Test
  public void testCreateFixedServerStore() throws Exception {
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
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), contains(client));
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));

    /*
     * Ensure the fixed resource pool remains after client disconnect.
     */
    activeEntity.disconnected(client);

    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(activeEntity.getConnectedClients().get(client), is(nullValue()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testCreateFixedServerStoreAfterValidate() throws Exception {
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
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client2), contains("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), contains(client2));
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testCreateFixedServerStoreExisting() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));
    activeEntity.disconnected(client);
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("cacheAlias"));
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())),
        IllegalStateException.class, "already exists");
  }

  @Test
  public void testCreateReleaseFixedServerStoreMultiple() throws Exception {
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
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("secondCache",
            new ServerStoreConfigBuilder()
                .fixed("serverResource2", 8, MemoryUnit.MEGABYTES)
                .build())));

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("cacheAlias", "secondCache"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L + 8L)));

    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("cacheAlias", "secondCache"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), contains(client));
    assertThat(activeEntity.getInUseStores().get("secondCache"), contains(client));
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias", "secondCache"));

    /*
     * Separately release the stores ...
     */
    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("cacheAlias")));

    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("cacheAlias", "secondCache"));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("secondCache"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(activeEntity.getInUseStores().get("secondCache"), contains(client));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.releaseServerStore("cacheAlias")),
        IllegalStateException.class, "not in use");

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("secondCache")));

    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("cacheAlias", "secondCache"));
    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(activeEntity.getInUseStores().get("secondCache"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias", "secondCache"));
  }

  @Test
  public void testValidateFixedServerStore() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);
    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
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

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().get(client2), contains("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), containsInAnyOrder(client, client2));
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testValidateFixedServerStoreBad() throws Exception {
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
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.validateServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .fixed("serverResource1", 8, MemoryUnit.MEGABYTES)
                .build())),
        ClusteredStoreValidationException.class);

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().get(client2), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), containsInAnyOrder(client));
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testValidateFixedServerStoreBeforeCreate() throws Exception {
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
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())),
        IllegalStateException.class, "does not exist");

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().get(client2), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), is(nullValue()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
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
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));

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
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));
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
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));

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
        IllegalStateException.class, "already exists");
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

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), contains(client));
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    assertSuccess(activeEntity.invoke(client2,
        MESSAGE_FACTORY.validateServerStore("cacheAlias", sharedStoreConfig)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().get(client2), contains("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), containsInAnyOrder(client, client2));
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));
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
        ClusteredStoreValidationException.class);

    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getConnectedClients().get(client), contains("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().get(client2), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), contains(client));
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testReleaseServerStoreBeforeAttach() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
        IllegalStateException.class, "does not exist");
  }

  @Test
  public void testReleaseServerStoreAfterRelease() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));
    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("cacheAlias"));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), containsInAnyOrder(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("cacheAlias")));

    assertThat(activeEntity.getStores(), containsInAnyOrder("cacheAlias"));
    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().get("cacheAlias"), is(Matchers.<ClientDescriptor>empty()));

    assertFailure(activeEntity.invoke(client,
        MESSAGE_FACTORY.releaseServerStore("cacheAlias")),
        IllegalStateException.class, "not in use by client");
  }

  @Test
  public void testDestroyServerStore() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
        MESSAGE_FACTORY.createServerStore("fixedCache",
            new ServerStoreConfigBuilder()
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));
    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("fixedCache"));
    assertThat(activeEntity.getInUseStores().get("fixedCache"), containsInAnyOrder(client));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("fixedCache")));

    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("fixedCache"));
    assertThat(activeEntity.getInUseStores().get("fixedCache"), is(Matchers.<ClientDescriptor>empty()));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("sharedCache",
            new ServerStoreConfigBuilder()
                .shared("secondary")
                .build())));
    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache", "sharedCache"));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder("fixedCache", "sharedCache"));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("sharedCache")));

    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache", "sharedCache"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("fixedCache"));
    assertThat(activeEntity.getInUseStores().get("fixedCache"), is(Matchers.<ClientDescriptor>empty()));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.destroyServerStore("sharedCache")));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache"));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder("fixedCache"));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.destroyServerStore("fixedCache")));

    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));
  }

  /**
   * Tests the destroy server store operation <b>before</b> the use of either a
   * {@link org.ehcache.clustered.common.messages.LifecycleMessage.CreateServerStore CreateServerStore}
   * {@link org.ehcache.clustered.common.messages.LifecycleMessage.ValidateServerStore ValidateServerStore}
   * operation.
   */
  @Test
  public void testDestroyServerStoreBeforeAttach() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
        IllegalStateException.class, "does not exist");
  }

  @Test
  public void testDestroyServerStoreInUse() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
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
        MESSAGE_FACTORY.createServerStore("fixedCache",
            new ServerStoreConfigBuilder()
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));
    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("sharedCache",
            new ServerStoreConfigBuilder()
                .shared("secondary")
                .build())));
    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache", "sharedCache"));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("fixedCache", "sharedCache"));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder("fixedCache", "sharedCache"));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration)));

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.destroyServerStore("sharedCache")),
        IllegalStateException.class, "in use");

    assertFailure(activeEntity.invoke(client2,
        MESSAGE_FACTORY.destroyServerStore("fixedCache")),
        IllegalStateException.class, "in use");

    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache", "sharedCache"));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("fixedCache", "sharedCache"));
    assertThat(activeEntity.getInUseStores().get("fixedCache"), contains(client));
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
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("fixedCache",
            new ServerStoreConfigBuilder()
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("fixedCache"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("fixedCache"));
    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache"));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("sharedCache",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build())));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("fixedCache", "sharedCache"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("fixedCache"));
    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache", "sharedCache"));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("primary",
            new ServerStoreConfigBuilder()
                .fixed("serverResource2", 4, MemoryUnit.MEGABYTES)
                .build())));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("fixedCache", "sharedCache", "primary"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("fixedCache", "primary"));
    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache", "sharedCache", "primary"));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("fixedCache")));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("sharedCache", "primary"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("fixedCache", "primary"));
    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache", "sharedCache", "primary"));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("primary")));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("sharedCache"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("fixedCache", "primary"));
    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache", "sharedCache", "primary"));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.releaseServerStore("sharedCache")));
    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(activeEntity.getFixedResourcePoolIds(), containsInAnyOrder("fixedCache", "primary"));
    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache", "sharedCache", "primary"));
  }

  @Test
  public void testDestroyEmpty() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(mock(ServiceRegistry.class), ENTITY_ID);
    activeEntity.destroy();
  }

  @Test
  public void testDestroyWithStores() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

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
        MESSAGE_FACTORY.createServerStore("fixedCache",
            new ServerStoreConfigBuilder()
                .fixed("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build())));
    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertSuccess(activeEntity.invoke(client,
        MESSAGE_FACTORY.createServerStore("sharedCache",
            new ServerStoreConfigBuilder()
                .shared("secondary")
                .build())));
    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache", "sharedCache"));
    assertThat(activeEntity.getConnectedClients().get(client), containsInAnyOrder("fixedCache", "sharedCache"));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder("fixedCache", "sharedCache"));

    activeEntity.disconnected(client);

    assertThat(activeEntity.getStores(), containsInAnyOrder("fixedCache", "sharedCache"));
    assertThat(activeEntity.getConnectedClients().keySet(), is(Matchers.<ClientDescriptor>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), containsInAnyOrder("fixedCache", "sharedCache"));
    assertThat(activeEntity.getInUseStores().get("fixedCache"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(activeEntity.getInUseStores().get("sharedCache"), is(Matchers.<ClientDescriptor>empty()));
    assertThat(activeEntity.getFixedResourcePoolIds(), contains("fixedCache"));
    assertThat(activeEntity.getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client2));

    activeEntity.destroy();

    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().get(client2), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getFixedResourcePoolIds(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getSharedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(0L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(0L)));
  }

  @Test
  public void testValidateSharedPoolSizeDifferent() throws Exception {
    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ServerSideConfiguration clientSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 8, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfig)));

    activeEntity.disconnected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), is(Matchers.<ClientDescriptor>empty()));

    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertFailure(activeEntity.invoke(client, MESSAGE_FACTORY.validateStoreManager(clientSideConfig)),IllegalArgumentException.class,"SharedPoolResources aren't valid. ServerSideConfiguration pool sent by client is different than server ServerSideConfiguration pool.");

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidateSharedPoolNamesDifferent() throws Exception {
    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ServerSideConfiguration clientSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("ternary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfig)));

    activeEntity.disconnected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), is(Matchers.<ClientDescriptor>empty()));

    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertFailure(activeEntity.invoke(client,MESSAGE_FACTORY.validateStoreManager(clientSideConfig)),IllegalArgumentException.class,"SharedPoolResources aren't valid. pool names not equal.");

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidateDefaultResourceNameDifferent() throws Exception {
    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource1")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ServerSideConfiguration clientSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource2")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,MESSAGE_FACTORY.configureStoreManager(serverSideConfig)));

    activeEntity.disconnected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), is(Matchers.<ClientDescriptor>empty()));

    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertFailure(activeEntity.invoke(client, MESSAGE_FACTORY.validateStoreManager(clientSideConfig)),IllegalArgumentException.class, "Default resource not aligned");

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidateClientSharedPoolSizeTooBig() throws Exception {
    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource1")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 32, MemoryUnit.MEGABYTES)
        .build();
    ServerSideConfiguration clientSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource1")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 36, MemoryUnit.MEGABYTES)
        .build();

    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client,MESSAGE_FACTORY.configureStoreManager(serverSideConfig)));

    activeEntity.disconnected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), is(Matchers.<ClientDescriptor>empty()));

    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertFailure(activeEntity.invoke(client,MESSAGE_FACTORY.validateStoreManager(clientSideConfig)),IllegalArgumentException.class, "SharedPoolResources aren't valid. ServerSideConfiguration pool sent by client is different than server ServerSideConfiguration pool.");

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidateSecondClientInheritsFirstClientConfig() throws Exception {
    ServerSideConfiguration serverSideConfig1 = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ServerSideConfiguration serverSideConfig2 = new ServerSideConfigBuilder()
        .build();
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);

    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);
    assertThat(activeEntity.getConnectedClients().keySet(), contains(client));

    assertSuccess(activeEntity.invoke(client, MESSAGE_FACTORY.configureStoreManager(serverSideConfig1)));

    ClientDescriptor client2 = new TestClientDescriptor();
    activeEntity.connected(client2);
    assertThat(activeEntity.getConnectedClients().keySet(), containsInAnyOrder(client, client2));

    assertSuccess(activeEntity.invoke(client2, MESSAGE_FACTORY.validateStoreManager(serverSideConfig2)));

    assertThat(activeEntity.getConnectedClients().get(client), is(Matchers.<String>empty()));
    assertThat(activeEntity.getConnectedClients().get(client2), is(Matchers.<String>empty()));
    assertThat(activeEntity.getInUseStores().keySet(), is(Matchers.<String>empty()));
    assertThat(activeEntity.getStores(), is(Matchers.<String>empty()));
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
    return new Pool(resourceName, unit.toBytes(poolSize));
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
      return new ServerSideConfiguration(defaultServerResource, pools);
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

    ServerStoreConfigBuilder fixed(String resourceName, int size, MemoryUnit unit) {
      this.poolAllocation = new PoolAllocation.Fixed(resourceName, unit.toBytes(size));
      return this;
    }

    ServerStoreConfigBuilder shared(String resourcePoolName) {
      this.poolAllocation = new PoolAllocation.Shared(resourcePoolName);
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

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getService(ServiceConfiguration<T> serviceConfiguration) {
      if (serviceConfiguration instanceof OffHeapResourceIdentifier) {
        final OffHeapResourceIdentifier resourceIdentifier = (OffHeapResourceIdentifier) serviceConfiguration;
        TestOffHeapResource offHeapResource = this.pools.get(resourceIdentifier);
        if (offHeapResource == null && offHeapSize != 0) {
          offHeapResource = new TestOffHeapResource(offHeapSize);
          this.pools.put(resourceIdentifier, offHeapResource);
        }
        return (T) offHeapResource;    // unchecked
      } else if (serviceConfiguration.getServiceType().equals(ClientCommunicator.class)) {
        return (T) mock(ClientCommunicator.class);
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

    private long getUsed() {
      return used;
    }
  }
}
