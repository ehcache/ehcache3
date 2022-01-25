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

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.exceptions.DestroyInProgressException;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerSideConfigurationException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Failure;
import org.ehcache.clustered.common.internal.messages.EhcacheResponseType;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageFactory;
import org.ehcache.clustered.server.management.Management;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.config.EhcacheStateServiceConfig;
import org.ehcache.clustered.server.store.InvalidMessage;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.management.service.monitoring.EntityManagementRegistryConfiguration;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResourceIdentifier;
import org.terracotta.offheapresource.OffHeapResources;
import org.terracotta.offheapstore.util.MemoryUnit;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ClusterTierManagerActiveEntityTest {

  private static final LifeCycleMessageFactory MESSAGE_FACTORY = new LifeCycleMessageFactory();
  private static final KeySegmentMapper DEFAULT_MAPPER = new KeySegmentMapper(16);

  private Management management = mock(Management.class);
  private ClusterTierManagerConfiguration blankConfiguration = new ClusterTierManagerConfiguration("identifier", new ServerSideConfigBuilder().build());

  @Test(expected = ConfigurationException.class)
  public void testConfigNull() throws Exception {
    new ClusterTierManagerActiveEntity(null, null, null);
  }

  /**
   * Ensures a disconnect for a non-connected client does not throw.
   */
  @Test
  public void testDisconnectedNotConnected() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(blankConfiguration, registry, DEFAULT_MAPPER));
    final ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(blankConfiguration, ehcacheStateService, management);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.disconnected(client);
    // Not expected to fail ...
  }

  /**
   * Ensures basic shared resource pool configuration.
   */
  @Test
  public void testConfigure() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
      .defaultResource("defaultServerResource")
      .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
      .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
      .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("identifier", serverSideConfiguration);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    final ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);
    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

    assertThat(registry.getStoreManagerService().getStores(), is(empty()));
  }

  /**
   * Ensure configuration fails when specifying non-existent resource.
   */
  @Test
  public void testConfigureMissingPoolResource() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("defaultServerResource", 64, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
      .defaultResource("defaultServerResource")
      .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
      .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)    // missing on 'server'
      .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("identifier", serverSideConfiguration);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    try {
      new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);
      fail("Entity creation should have failed");
    } catch (ConfigurationException e) {
      assertThat(e.getMessage(), containsString("Non-existent server side resource"));
    }

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), is(empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(0L));
    assertThat(registry.getResource("serverResource2"), is(nullValue()));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

    assertThat(registry.getStoreManagerService().getStores(), is(empty()));
  }

  /**
   * Ensure configuration fails when specifying non-existent default resource.
   */
  @Test
  public void testConfigureMissingDefaultResource() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
      .defaultResource("defaultServerResource")
      .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
      .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
      .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("idenitifier", serverSideConfiguration);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    try {
      new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);
      fail("Entity creation should have failed");
    } catch (ConfigurationException e) {
      assertThat(e.getMessage(), containsString("Default server resource"));
    }
    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), is(empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(0L));
    assertThat(registry.getResource("serverResource2").getUsed(), is(0L));
    assertThat(registry.getResource("defaultServerResource"), is(nullValue()));

    assertThat(registry.getStoreManagerService().getStores(), is(empty()));
  }

  @Test
  public void testConfigureLargeSharedPool() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource", 64, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
      .defaultResource("defaultServerResource")
      .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
      .sharedPool("secondary", "serverResource2", 4, MemoryUnit.MEGABYTES)
      .sharedPool("tooBig", "serverResource2", 16, MemoryUnit.MEGABYTES)
      .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("identifier", serverSideConfiguration);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    try {
      new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);
      fail("Entity creation should have failed");
    } catch (ConfigurationException e) {
      assertThat(e.getMessage(), containsString("resources to allocate"));
    }

    final Set<String> poolIds = registry.getStoreManagerService().getSharedResourcePoolIds();
    assertThat(poolIds, is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(0L));
    assertThat(registry.getResource("serverResource2").getUsed(), is(0L));
    assertThat(registry.getResource("defaultServerResource").getUsed(), is(0L));

    assertThat(registry.getStoreManagerService().getStores(), is(Matchers.<String>empty()));
  }

  @Test
  public void testValidate2Clients() throws Exception {
    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("idenitifer", serverSideConfig);
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    final ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);
    TestInvokeContext context = new TestInvokeContext();
    activeEntity.connected(context.getClientDescriptor());

    assertSuccess(activeEntity.invokeActive(context, MESSAGE_FACTORY.validateStoreManager(serverSideConfig)));

    TestInvokeContext context2 = new TestInvokeContext();
    activeEntity.connected(context2.getClientDescriptor());

    assertSuccess(activeEntity.invokeActive(context2, MESSAGE_FACTORY.validateStoreManager(serverSideConfig)));
  }

  @Test
  public void testValidateAfterConfigure() throws Exception {
    ServerSideConfiguration serverSideConfig = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("identifier", serverSideConfig);
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    final ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);
    TestInvokeContext context = new TestInvokeContext();
    activeEntity.connected(context.getClientDescriptor());
    assertSuccess(activeEntity.invokeActive(context, MESSAGE_FACTORY.validateStoreManager(serverSideConfig)));
  }

  @Test
  public void testValidateExtraResource() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
      .defaultResource("defaultServerResource")
      .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
      .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
      .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("identifier", serverSideConfiguration);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    final ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);
    TestInvokeContext context = new TestInvokeContext();
    activeEntity.connected(context.getClientDescriptor());

    assertFailure(activeEntity.invokeActive(context,
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
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
      .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
      .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
      .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("identifier", serverSideConfiguration);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    final ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);
    TestInvokeContext context = new TestInvokeContext();
    activeEntity.connected(context.getClientDescriptor());

    assertFailure(activeEntity.invokeActive(context,
        MESSAGE_FACTORY.validateStoreManager(new ServerSideConfigBuilder()
            .defaultResource("defaultServerResource")
            .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
            .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
            .build())),
        InvalidServerSideConfigurationException.class, "Default resource not aligned");
  }

  @Test
  public void testDestroyEmpty() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(blankConfiguration, registry, DEFAULT_MAPPER));
    final ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(blankConfiguration, ehcacheStateService, management);
    activeEntity.destroy();
  }

  @Test
  public void testValidateIdenticalConfiguration() throws Exception {
    ServerSideConfiguration configureConfig = new ServerSideConfigBuilder()
        .defaultResource("primary-server-resource")
        .sharedPool("foo", null, 8, MemoryUnit.MEGABYTES)
        .sharedPool("bar", "secondary-server-resource", 8, MemoryUnit.MEGABYTES)
        .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("identifier", configureConfig);

    ServerSideConfiguration validateConfig = new ServerSideConfigBuilder()
        .defaultResource("primary-server-resource")
        .sharedPool("foo", null, 8, MemoryUnit.MEGABYTES)
        .sharedPool("bar", "secondary-server-resource", 8, MemoryUnit.MEGABYTES)
        .build();

    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("primary-server-resource", 16, MemoryUnit.MEGABYTES);
    registry.addResource("secondary-server-resource", 16, MemoryUnit.MEGABYTES);

    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    final ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);

    TestInvokeContext context = new TestInvokeContext();
    activeEntity.connected(context.getClientDescriptor());

    assertThat(activeEntity.invokeActive(context, MESSAGE_FACTORY.validateStoreManager(validateConfig)).getResponseType(), is(EhcacheResponseType.SUCCESS));
  }

  @Test
  public void testValidateSharedPoolNamesDifferent() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource", 64, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    ServerSideConfiguration configure = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("identifier", configure);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);

    TestInvokeContext context = new TestInvokeContext();
    activeEntity.connected(context.getClientDescriptor());
    ServerSideConfiguration validate = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("ternary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertFailure(activeEntity.invokeActive(context, MESSAGE_FACTORY.validateStoreManager(validate)), InvalidServerSideConfigurationException.class, "Pool names not equal.");
  }

  @Test
  public void testValidateDefaultResourceNameDifferent() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    ServerSideConfiguration configure = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource1")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("identifier", configure);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);

    TestInvokeContext context = new TestInvokeContext();
    activeEntity.connected(context.getClientDescriptor());
    ServerSideConfiguration validate = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource2")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    assertFailure(activeEntity.invokeActive(context, MESSAGE_FACTORY.validateStoreManager(validate)), InvalidServerSideConfigurationException.class, "Default resource not aligned.");
  }

  @Test
  public void testValidateClientSharedPoolSizeTooBig() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    ServerSideConfiguration configure = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource1")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("identifier", configure);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);

    TestInvokeContext context = new TestInvokeContext();
    activeEntity.connected(context.getClientDescriptor());
    ServerSideConfiguration validate = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource1")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 36, MemoryUnit.MEGABYTES)
        .build();
    assertFailure(activeEntity.invokeActive(context, MESSAGE_FACTORY.validateStoreManager(validate)),InvalidServerSideConfigurationException.class, "Pool 'secondary' not equal.");
  }

  @Test
  public void testValidateSecondClientInheritsFirstClientConfig() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("defaultServerResource", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 8, MemoryUnit.MEGABYTES);

    ServerSideConfiguration initialConfiguration = new ServerSideConfigBuilder()
        .defaultResource("defaultServerResource")
        .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
        .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
        .build();
    ClusterTierManagerConfiguration configuration = new ClusterTierManagerConfiguration("identifier", initialConfiguration);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(configuration, ehcacheStateService, management);

    TestInvokeContext context = new TestInvokeContext();
    activeEntity.connected(context.getClientDescriptor());
    assertSuccess(activeEntity.invokeActive(context, MESSAGE_FACTORY.validateStoreManager(null)));
  }

  @Test
  public void testInvalidMessageThrowsError() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);

    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(blankConfiguration, registry, DEFAULT_MAPPER));
    final ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(blankConfiguration, ehcacheStateService, management);

    TestInvokeContext context = new TestInvokeContext();
    activeEntity.connected(context.getClientDescriptor());

    try {
      activeEntity.invokeActive(context, new InvalidMessage());
      fail("Invalid message should result in AssertionError");
    } catch (AssertionError e) {
      assertThat(e.getMessage(), containsString("Unsupported"));
    }
  }

  @Test
  public void testPrepareForDestroy() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);

    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(blankConfiguration, registry, DEFAULT_MAPPER));
    final ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(blankConfiguration, ehcacheStateService, management);

    TestInvokeContext context = new TestInvokeContext();
    activeEntity.connected(context.getClientDescriptor());

    activeEntity.invokeActive(context, MESSAGE_FACTORY.prepareForDestroy());

    try {
      ehcacheStateService.validate(null);
      fail("DestroyInProgressException expected");
    } catch (DestroyInProgressException e) {
      assertThat(e.getMessage(), containsString("in progress for destroy"));
    }
  }

  @Test
  public void testPrepareForDestroyInProgress() throws Exception {
    final OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry();
    registry.addResource("serverResource1", 8, MemoryUnit.MEGABYTES);

    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(blankConfiguration, registry, DEFAULT_MAPPER));
    ehcacheStateService.prepareForDestroy();

    final ClusterTierManagerActiveEntity activeEntity = new ClusterTierManagerActiveEntity(blankConfiguration, ehcacheStateService, management);

    TestInvokeContext context = new TestInvokeContext();
    activeEntity.connected(context.getClientDescriptor());

    assertFailure(activeEntity.invokeActive(context, MESSAGE_FACTORY.validateStoreManager(null)), DestroyInProgressException.class, "in progress for destroy");
  }


  private void assertSuccess(EhcacheEntityResponse response) throws Exception {
    if (!EhcacheResponseType.SUCCESS.equals(response.getResponseType())) {
      throw ((Failure) response).getCause();
    }
  }

  private void assertFailure(EhcacheEntityResponse response, Class<? extends Exception> expectedException, String expectedMessageContent) {
    assertThat(response.getResponseType(), is(EhcacheResponseType.FAILURE));
    Exception cause = ((Failure) response).getCause();
    assertThat(cause, is(instanceOf(expectedException)));
    assertThat(cause.getMessage(), containsString(expectedMessageContent));
  }

  private static Pool pool(String resourceName, int poolSize, MemoryUnit unit) {
    return new Pool(unit.toBytes(poolSize), resourceName);
  }

  private static final class ServerSideConfigBuilder {
    private final Map<String, Pool> pools = new HashMap<>();
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
   * Provides a {@link ServiceRegistry} for off-heap resources.  This is a "server-side" object.
   */
  private static final class OffHeapIdentifierRegistry implements ServiceRegistry {

    private EhcacheStateServiceImpl storeManagerService;

    private final Map<OffHeapResourceIdentifier, TestOffHeapResource> pools =
      new HashMap<>();

    /**
     * Instantiate a "closed" {@code ServiceRegistry}.  Using this constructor creates a
     * registry that only returns {@code OffHeapResourceIdentifier} entries supplied
     * through the {@link #addResource} method.
     */
    private OffHeapIdentifierRegistry() {
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
      Set<String> names = new HashSet<>();
      for (OffHeapResourceIdentifier identifier: pools) {
        names.add(identifier.getName());
      }

      return Collections.unmodifiableSet(names);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getService(ServiceConfiguration<T> serviceConfiguration) {
      if (serviceConfiguration.getServiceType().equals(EhcacheStateService.class)) {
        EhcacheStateServiceConfig config = (EhcacheStateServiceConfig) serviceConfiguration;
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
          }, config.getConfig().getConfiguration(), DEFAULT_MAPPER, service -> {});
        }
        return (T) (this.storeManagerService);
      } else if(serviceConfiguration instanceof EntityManagementRegistryConfiguration) {
        return null;
      }

      throw new UnsupportedOperationException("Registry.getService does not support " + serviceConfiguration.getClass().getName());
    }

    @Override
    public <T> Collection<T> getServices(ServiceConfiguration<T> configuration) {
      return Collections.singleton(getService(configuration));
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

    @Override
    public boolean setCapacity(long size) throws IllegalArgumentException {
      throw new UnsupportedOperationException("Not supported");
    }

    private long getUsed() {
      return used;
    }
  }
}
