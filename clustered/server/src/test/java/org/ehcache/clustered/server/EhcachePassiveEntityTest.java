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
import org.ehcache.clustered.common.internal.ClusteredTierManagerConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageFactory;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.server.management.Management;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.config.EhcacheStateServiceConfig;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.entity.BasicServiceConfiguration;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.management.service.monitoring.ConsumerManagementRegistryConfiguration;
import org.terracotta.management.service.monitoring.PassiveEntityMonitoringServiceConfiguration;
import org.terracotta.monitoring.IMonitoringProducer;
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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class EhcachePassiveEntityTest {

  private static final LifeCycleMessageFactory MESSAGE_FACTORY = new LifeCycleMessageFactory();
  private static final UUID CLIENT_ID = UUID.randomUUID();
  private static final KeySegmentMapper DEFAULT_MAPPER = new KeySegmentMapper(16);

  @Before
  public void setClientId() {
    MESSAGE_FACTORY.setClientId(CLIENT_ID);
  }

  @Test(expected = ConfigurationException.class)
  public void testConfigNull() throws Exception {
    new EhcachePassiveEntity(null, null, null);
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

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
      .defaultResource("defaultServerResource")
      .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
      .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
      .build();
    ClusteredTierManagerConfiguration configuration = new ClusteredTierManagerConfiguration("identifier", serverSideConfiguration);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    Management management = new Management(registry, ehcacheStateService, false);
    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(configuration, ehcacheStateService, management);

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

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
      .defaultResource("defaultServerResource")
      .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
      .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)    // missing on 'server'
      .build();
    ClusteredTierManagerConfiguration configuration = new ClusteredTierManagerConfiguration("identifier", serverSideConfiguration);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    Management management = new Management(registry, ehcacheStateService, false);
    try {
      new EhcachePassiveEntity(configuration, ehcacheStateService, management);
      fail("Entity creation should have failed");
    } catch (ConfigurationException e) {
      assertThat(e.getMessage(), containsString("server side resource"));
    }

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

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
      .defaultResource("defaultServerResource")
      .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
      .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
      .build();
    ClusteredTierManagerConfiguration configuration = new ClusteredTierManagerConfiguration("identifier", serverSideConfiguration);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    Management management = new Management(registry, ehcacheStateService, false);
    try {
      new EhcachePassiveEntity(configuration, ehcacheStateService, management);
      fail("Entity creation should have failed");
    } catch (ConfigurationException e) {
      assertThat(e.getMessage(), containsString("not defined"));
    }

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

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
      .defaultResource("defaultServerResource")
      .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
      .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
      .sharedPool("tooBig", "serverResource2", 64, MemoryUnit.MEGABYTES)
      .build();
    ClusteredTierManagerConfiguration configuration = new ClusteredTierManagerConfiguration("identifier", serverSideConfiguration);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    Management management = new Management(registry, ehcacheStateService, false);
    try {
      new EhcachePassiveEntity(configuration, ehcacheStateService, management);
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
  public void testDestroyWithStores() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource1", 32, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource2", 32, MemoryUnit.MEGABYTES);

    ServerSideConfiguration serverSideConfiguration = new ServerSideConfigBuilder()
      .sharedPool("primary", "serverResource1", 4, MemoryUnit.MEGABYTES)
      .sharedPool("secondary", "serverResource2", 8, MemoryUnit.MEGABYTES)
      .build();
    ClusteredTierManagerConfiguration configuration = new ClusteredTierManagerConfiguration("identifier", serverSideConfiguration);
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    Management management = new Management(registry, ehcacheStateService, false);
    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(configuration, ehcacheStateService, management);

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(4L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(8L)));

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), containsInAnyOrder("primary", "secondary"));

    passiveEntity.destroy();

    assertThat(registry.getStoreManagerService().getSharedResourcePoolIds(), is(Matchers.<String>empty()));

    assertThat(registry.getResource("serverResource1").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(0L)));
    assertThat(registry.getResource("serverResource2").getUsed(), is(MemoryUnit.MEGABYTES.toBytes(0L)));
  }

  @Test
  public void testInvalidMessageThrowsError() throws Exception {
    OffHeapIdentifierRegistry registry = new OffHeapIdentifierRegistry(4, MemoryUnit.MEGABYTES);
    registry.addResource("serverResource", 4, MemoryUnit.MEGABYTES);

    ClusteredTierManagerConfiguration configuration = new ClusteredTierManagerConfiguration("identifier", new ServerSideConfigBuilder()
      .build());
    EhcacheStateService ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(configuration, registry, DEFAULT_MAPPER));
    Management management = new Management(registry, ehcacheStateService, false);
    final EhcachePassiveEntity passiveEntity = new EhcachePassiveEntity(configuration, ehcacheStateService, management);

    try {
      passiveEntity.invoke(new InvalidMessage());
      fail("Invalid message should result in AssertionError");
    } catch (AssertionError e) {
      assertThat(e.getMessage(), containsString("Unsupported"));
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
      if (serviceConfiguration.getServiceType().equals(EhcacheStateService.class)) {
        EhcacheStateServiceConfig config = (EhcacheStateServiceConfig) serviceConfiguration;
        if (storeManagerService == null) {
          this.storeManagerService = new EhcacheStateServiceImpl(config.getConfig().getIdentifier(), new OffHeapResources() {
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
      } else if (serviceConfiguration.getServiceType().equals(IEntityMessenger.class)) {
        return (T) mock(IEntityMessenger.class);
      } else if(serviceConfiguration instanceof ConsumerManagementRegistryConfiguration) {
        return null;
      } else if(serviceConfiguration instanceof PassiveEntityMonitoringServiceConfiguration) {
        return null;
      } else if(serviceConfiguration instanceof BasicServiceConfiguration && serviceConfiguration.getServiceType() == IMonitoringProducer.class) {
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
