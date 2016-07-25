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

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageFactory;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreMessageFactory;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
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

import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class EhcachePassiveEntityTest {

  private static final byte[] ENTITY_ID = ClusteredEhcacheIdentity.serialize(UUID.randomUUID());
  private static final LifeCycleMessageFactory MESSAGE_FACTORY = new LifeCycleMessageFactory();

  @Test
  public void testConfigTooShort() {
    try {
      new EhcachePassiveEntity(null, new byte[ENTITY_ID.length - 1]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testConfigTooLong() {
    try {
      new EhcachePassiveEntity(null, new byte[ENTITY_ID.length + 1]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testConfigNull() {
    try {
      new EhcachePassiveEntity(null, null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build()));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testConfigureAfterConfigure() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build()));

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build()));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)    // missing on 'server'
        .build()));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(0L));
    assertThat(registry.getResource("serverResource2"), is(nullValue()));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build()));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(0L));
    assertThat(registry.getResource("serverResource2").getUsed(), is(0L));
    assertThat(registry.getResource("defaultServerResource"), is(nullValue()));

    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testConfigureLargeSharedPool() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource", 64, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .sharedPool("tooBig", "serverResource2", 64, MemoryUnit.MEGABYTES)
        .build()));

    final Set<String> poolIds = registry.getStoreManagerService().getSharedResourcePoolIds();
    assertThat(poolIds, is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(0L));
    assertThat(registry.getResource("serverResource2").getUsed(), is(0L));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfig));

    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(serverSideConfig));

    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testCreateDedicatedServerStoreBeforeConfigure() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.createServerStore("cacheAlias",
        new ServerStoreConfigBuilder()
            .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
            .build()));
  }

  @Test
  public void testCreateDedicatedServerStoreBeforeValidate() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build()));

    passiveEntity.invoke(MESSAGE_FACTORY.createServerStore("cacheAlias",
        new ServerStoreConfigBuilder()
            .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
            .build()));
  }

  @Test
  public void testCreateDedicatedServerStore() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build()));

    passiveEntity.invoke(MESSAGE_FACTORY.createServerStore("cacheAlias",
        new ServerStoreConfigBuilder()
            .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
            .build()));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    /*
     * Ensure the dedicated resource pool remains after client disconnect.
     */

    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfig));

    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(serverSideConfig));

    passiveEntity.invoke(MESSAGE_FACTORY.createServerStore("cacheAlias",
        new ServerStoreConfigBuilder()
            .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
            .build()));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testCreateDedicatedServerStoreExisting() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build()));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build()));
  }

  @Test
  public void testCreateReleaseDedicatedServerStoreMultiple() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build()));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build()));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("secondCache",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource2", 8, MemoryUnit.MEGABYTES)
                .build()));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService()
        .getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias", "secondCache"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L + 8L)));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias", "secondCache"));

    /*
     * Separately release the stores ...
     */
    passiveEntity.invoke(MESSAGE_FACTORY.releaseServerStore("cacheAlias"));

    assertThat(registry.getStoreManagerService()
        .getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias", "secondCache"));

    passiveEntity.invoke(
        MESSAGE_FACTORY.releaseServerStore("cacheAlias"));

    passiveEntity.invoke(MESSAGE_FACTORY.releaseServerStore("secondCache"));

    assertThat(registry.getStoreManagerService()
        .getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias", "secondCache"));
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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration));

    passiveEntity.invoke(MESSAGE_FACTORY.createServerStore("cacheAlias", serverStoreConfiguration));


    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration));

    passiveEntity.invoke(MESSAGE_FACTORY.validateServerStore("cacheAlias", serverStoreConfiguration));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build()));

    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration));

    passiveEntity.invoke(
        MESSAGE_FACTORY.validateServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 8, MemoryUnit.MEGABYTES)
                .build()));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfig));


    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(serverSideConfig));

    passiveEntity.invoke(
        MESSAGE_FACTORY.validateServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build()));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testCreateSharedServerStore() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build()));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build()));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    /*
     * Ensure the shared resource store remains after client disconnect.
     */
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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(
        MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build()));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    passiveEntity.invoke(
        MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build()));
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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("cacheAlias", sharedStoreConfig));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration));

    passiveEntity.invoke(
        MESSAGE_FACTORY.validateServerStore("cacheAlias", sharedStoreConfig));

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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration));

    passiveEntity.invoke(MESSAGE_FACTORY.createServerStore("cacheAlias", dedicatedStoreConfig1));

    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration));

    String expectedMessageContent = "Existing ServerStore configuration is not compatible with the desired configuration: " +
                                    "\n\t" +
                                    "resourcePoolDedicatedSize existing: " +
                                    ((PoolAllocation.Dedicated)dedicatedStoreConfig1.getPoolAllocation()).getSize() +
                                    ", desired: " +
                                    ((PoolAllocation.Dedicated)dedicatedStoreConfig2.getPoolAllocation()).getSize();

    passiveEntity.invoke(MESSAGE_FACTORY.validateServerStore("cacheAlias", dedicatedStoreConfig2));
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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration));

    passiveEntity.invoke(MESSAGE_FACTORY.createServerStore("cacheAlias", dedicatedStoreConfig1));


    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration));
    passiveEntity.invoke(MESSAGE_FACTORY.validateServerStore("cacheAlias", dedicatedStoreConfig2));

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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration));

    passiveEntity.invoke(MESSAGE_FACTORY.createServerStore("cacheAlias", dedicatedStoreConfig1));

    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration));
    String expectedMessageContent = "Existing ServerStore configuration is not compatible with the desired configuration: " +
                                    "\n\t" +
                                    "resourcePoolDedicatedResourceName existing: " +
                                    ((PoolAllocation.Dedicated)dedicatedStoreConfig1.getPoolAllocation()).getResourceName() +
                                    ", desired: " +
                                    ((PoolAllocation.Dedicated)dedicatedStoreConfig2.getPoolAllocation()).getResourceName();

    passiveEntity.invoke(MESSAGE_FACTORY.validateServerStore("cacheAlias", dedicatedStoreConfig2));
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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration));

    passiveEntity.invoke(MESSAGE_FACTORY.createServerStore("cacheAlias", dedicatedStoreConfig1));

    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration));
    passiveEntity.invoke(MESSAGE_FACTORY.validateServerStore("cacheAliasBad", dedicatedStoreConfig2));

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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(
        MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build()));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    passiveEntity.invoke(
        MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("secondary")
                .build()));
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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build()));

    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(serverSideConfiguration));

    passiveEntity.invoke(
        MESSAGE_FACTORY.validateServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .shared("secondary")
                .build()));

    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));
  }

  @Test
  public void testReleaseServerStoreBeforeAttach() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build()));

    passiveEntity.invoke(
        MESSAGE_FACTORY.releaseServerStore("cacheAlias"));
  }

  @Test
  public void testReleaseServerStoreAfterRelease() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build()));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("cacheAlias",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build()));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    passiveEntity.invoke(MESSAGE_FACTORY.releaseServerStore("cacheAlias"));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("cacheAlias"));

    passiveEntity.invoke(
        MESSAGE_FACTORY.releaseServerStore("cacheAlias"));
  }

  @Test
  public void testDestroyServerStore() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build()));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("dedicatedCache",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build()));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    passiveEntity.invoke(MESSAGE_FACTORY.releaseServerStore("dedicatedCache"));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache"));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("sharedCache",
            new ServerStoreConfigBuilder()
                .shared("secondary")
                .build()));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache"));

    passiveEntity.invoke(MESSAGE_FACTORY.releaseServerStore("sharedCache"));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache"));

    passiveEntity.invoke(MESSAGE_FACTORY.destroyServerStore("sharedCache"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache"));

    passiveEntity.invoke(MESSAGE_FACTORY.destroyServerStore("dedicatedCache"));

    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), is(Matchers.<String>empty()));

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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build()));

    passiveEntity.invoke(
        MESSAGE_FACTORY.destroyServerStore("cacheAlias"));
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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build()));
    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("dedicatedCache",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build()));
    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache"));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache"));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("sharedCache",
            new ServerStoreConfigBuilder()
                .shared("primary")
                .build()));
    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache"));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache"));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("primary",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource2", 4, MemoryUnit.MEGABYTES)
                .build()));
    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService()
        .getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache", "primary"));
    assertThat(registry.getStoreManagerService()
        .getStores(), containsInAnyOrder("dedicatedCache", "sharedCache", "primary"));

    passiveEntity.invoke(MESSAGE_FACTORY.releaseServerStore("dedicatedCache"));
    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService()
        .getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache", "primary"));
    assertThat(registry.getStoreManagerService()
        .getStores(), containsInAnyOrder("dedicatedCache", "sharedCache", "primary"));

    passiveEntity.invoke(MESSAGE_FACTORY.releaseServerStore("primary"));
    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService()
        .getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache", "primary"));
    assertThat(registry.getStoreManagerService()
        .getStores(), containsInAnyOrder("dedicatedCache", "sharedCache", "primary"));

    passiveEntity.invoke(MESSAGE_FACTORY.releaseServerStore("sharedCache"));
    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));
    assertThat(registry.getStoreManagerService()
        .getDedicatedResourcePoolIds(), containsInAnyOrder("dedicatedCache", "primary"));
    assertThat(registry.getStoreManagerService()
        .getStores(), containsInAnyOrder("dedicatedCache", "sharedCache", "primary"));
  }

  @Test
  public void testDestroyEmpty() throws Exception {
    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(new OffHeapIdentifierRegistry(), ENTITY_ID);
    passiveEntity.destroy();
  }

  @Test
  public void testDestroyWithStores() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(
        MESSAGE_FACTORY.configureStoreManager(new ServerSideConfigBuilder()
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build()));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("dedicatedCache",
            new ServerStoreConfigBuilder()
                .dedicated("serverResource1", 4, MemoryUnit.MEGABYTES)
                .build()));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L + 4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    passiveEntity.invoke(
        MESSAGE_FACTORY.createServerStore("sharedCache",
            new ServerStoreConfigBuilder()
                .shared("secondary")
                .build()));
    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache"));

    assertThat(registry.getStoreManagerService().getStores(), containsInAnyOrder("dedicatedCache", "sharedCache"));
    assertThat(registry.getStoreManagerService().getDedicatedResourcePoolIds(), contains("dedicatedCache"));
    assertThat(registry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));

    passiveEntity.destroy();

    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
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

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(validateConfig));
  }

  @Test
  public void testValidateSharedPoolNamesDifferent() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 64, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    ServerSideConfiguration configure = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(configure));

    ServerSideConfiguration validate = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("ternary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(validate));
  }

  @Test
  public void testValidateDefaultResourceNameDifferent() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    ServerSideConfiguration configure = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource1")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(configure));

    ServerSideConfiguration validate = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource2")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(validate));
  }

  @Test
  public void testValidateClientSharedPoolSizeTooBig() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(64, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    ServerSideConfiguration configure = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource1")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 32, MemoryUnit.MEGABYTES)
        .build();
    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(configure));

    ServerSideConfiguration validate = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource1")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 36, MemoryUnit.MEGABYTES)
        .build();
    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(validate));
  }

  @Test
  public void testValidateSecondClientInheritsFirstClientConfig() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    ServerSideConfiguration initialConfiguration = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(initialConfiguration));

    passiveEntity.invoke(MESSAGE_FACTORY.validateStoreManager(null));
  }

  @Test
  public void testValidateNonExistentSharedPool() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);

    EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    ServerSideConfiguration configure = new ServerSideConfigBuilder().build();
    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(configure));

    ServerStoreConfiguration sharedStoreConfig = new ServerStoreConfigBuilder()
        .shared("non-existent-pool")
        .build();

    passiveEntity.invoke(MESSAGE_FACTORY.createServerStore("sharedCache", sharedStoreConfig));
  }

  public void testCreateServerStoreWithUnknownPool() throws Exception {

    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .build();
    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfigBuilder()
        .unknown()
        .build();

    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(registry, ENTITY_ID);

    passiveEntity.invoke(MESSAGE_FACTORY.configureStoreManager(serverSideConfiguration));

    try {
      passiveEntity.invoke(
          MESSAGE_FACTORY.createServerStore("cacheAlias", serverStoreConfiguration));
      fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertThat(e.getMessage(), is("Server Store can't be created with an Unknown resource pool"));
    }
  }

  private static ServerSideConfiguration.Pool pool(String resourceName, int poolSize, MemoryUnit unit) {
    return new ServerSideConfiguration.Pool(unit.toBytes(poolSize), resourceName);
  }

  private static final class ServerSideConfigBuilder {
    private final Map<String, ServerSideConfiguration.Pool> pools = new HashMap<String, ServerSideConfiguration.Pool>();
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

  /**
   * Provides a {@link ServiceRegistry} for off-heap resources.  This is a "server-side" object.
   */
  private static final class OffHeapIdentifierRegistry implements ServiceRegistry {

    private final long offHeapSize;

    private EhcacheStateServiceImpl storeManagerService;

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
      if (serviceConfiguration instanceof OffHeapResourceIdentifier) {
        final OffHeapResourceIdentifier resourceIdentifier = (OffHeapResourceIdentifier) serviceConfiguration;
        return (T) this.pools.get(resourceIdentifier);
//      } else if (serviceConfiguration.getServiceType().equals(ClientCommunicator.class)) {
//        return (T) mock(ClientCommunicator.class);
      } else if(serviceConfiguration.getServiceType().equals(OffHeapResources.class)) {
        return (T) new OffHeapResources() {
          @Override
          public Set<String> getAllIdentifiers() {
            return getIdentifiers(pools.keySet());
          }
        };
      } else if (serviceConfiguration.getServiceType().equals(EhcacheStateService.class)) {
        if (storeManagerService == null) {
          this.storeManagerService = new EhcacheStateServiceImpl(this, getIdentifiers(pools.keySet()));
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

    private long getUsed() {
      return used;
    }
  }
}
