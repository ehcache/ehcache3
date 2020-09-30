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
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.ClusterTierEntityConfiguration;
import org.ehcache.clustered.server.EhcacheStateServiceImpl;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.TestClientDescriptor;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.client.message.tracker.OOOMessageHandlerConfiguration;
import org.terracotta.client.message.tracker.OOOMessageHandlerImpl;
import org.terracotta.entity.BasicServiceConfiguration;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.management.service.monitoring.EntityManagementRegistryConfiguration;
import org.terracotta.monitoring.IMonitoringProducer;
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

import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.ehcache.clustered.ChainUtils.sequencedChainOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ClusterTierPassiveEntityTest {

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
    defaultRegistry = new OffHeapIdentifierRegistry();
    defaultRegistry.addResource(defaultResource, 10, MemoryUnit.MEGABYTES);
    defaultStoreConfiguration = new ServerStoreConfigBuilder().dedicated(defaultResource, 1024, MemoryUnit.KILOBYTES).build();
    defaultConfiguration = new ClusterTierEntityConfiguration(identifier, defaultStoreName,
      defaultStoreConfiguration);
  }

  @Test(expected = ConfigurationException.class)
  public void testConfigNull() throws Exception {
    new ClusterTierPassiveEntity(mock(ServiceRegistry.class), null, DEFAULT_MAPPER);
  }

  @Test
  public void testCreateDedicatedServerStore() throws Exception {
    ClusterTierPassiveEntity passiveEntity = new ClusterTierPassiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    passiveEntity.createNew();

    assertThat(defaultRegistry.getStoreManagerService().getDedicatedResourcePoolIds(), containsInAnyOrder(defaultStoreName));
    assertThat(defaultRegistry.getResource(defaultResource).getUsed(), is(MemoryUnit.MEGABYTES.toBytes(1L)));
    assertThat(defaultRegistry.getStoreManagerService().getStores(), containsInAnyOrder(defaultStoreName));
  }

  @Test
  public void testCreateSharedServerStore() throws Exception {
    defaultRegistry.addSharedPool(defaultSharedPool, MemoryUnit.MEGABYTES.toBytes(2), defaultResource);
    ServerStoreConfiguration storeConfiguration = new ServerStoreConfigBuilder().shared(defaultSharedPool).build();
    ClusterTierPassiveEntity passiveEntity = new ClusterTierPassiveEntity(defaultRegistry,
      new ClusterTierEntityConfiguration(identifier, defaultStoreName, storeConfiguration), DEFAULT_MAPPER);
    passiveEntity.createNew();

    assertThat(defaultRegistry.getStoreManagerService().getStores(), containsInAnyOrder(defaultStoreName));
    assertThat(defaultRegistry.getStoreManagerService()
        .getSharedResourcePoolIds(), containsInAnyOrder(defaultSharedPool));
    assertThat(defaultRegistry.getStoreManagerService().getDedicatedResourcePoolIds(), is(empty()));
    assertThat(defaultRegistry.getResource(defaultResource).getUsed(), is(MemoryUnit.MEGABYTES.toBytes(2L)));
  }

  @Test
  public void testDestroyServerStore() throws Exception {
    ClusterTierPassiveEntity passiveEntity = new ClusterTierPassiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    passiveEntity.createNew();

    passiveEntity.destroy();

    assertThat(defaultRegistry.getStoreManagerService().getStores(), is(empty()));
    assertThat(defaultRegistry.getStoreManagerService().getDedicatedResourcePoolIds(), is(empty()));

    assertThat(defaultRegistry.getResource(defaultResource).getUsed(), is(0L));
  }

  @Test
  public void testInvalidMessageThrowsError() throws Exception {
    ClusterTierPassiveEntity passiveEntity = new ClusterTierPassiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    TestClientDescriptor client = TestClientDescriptor.newClient();
    try {
      passiveEntity.invokePassive(client.invokeContext(), new InvalidMessage());
      fail("Invalid message should result in AssertionError");
    } catch (AssertionError e) {
      assertThat(e.getMessage(), containsString("Unsupported"));
    }
  }

  @Test
  public void testPassiveTracksMessageDuplication() throws Exception {
    ClusterTierPassiveEntity passiveEntity = new ClusterTierPassiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    passiveEntity.createNew();

    Chain chain = sequencedChainOf(createPayload(1L));
    TestClientDescriptor client = TestClientDescriptor.newClient();

    long clientId = 3;

    PassiveReplicationMessage message1 = new PassiveReplicationMessage.ChainReplicationMessage(2, chain, 2L, 1L, clientId);
    passiveEntity.invokePassive(client.invokeContext(), message1);

    // Should be added
    assertThat(passiveEntity.getStateService().getStore(passiveEntity.getStoreIdentifier()).get(2).isEmpty(), is(false));

    Chain emptyChain = sequencedChainOf();
    PassiveReplicationMessage message2 = new PassiveReplicationMessage.ChainReplicationMessage(2, emptyChain, 2L, 1L, clientId);
    passiveEntity.invokePassive(client.invokeContext(), message2);

    // Should not be cleared, message is a duplicate
    assertThat(passiveEntity.getStateService().getStore(passiveEntity.getStoreIdentifier()).get(2).isEmpty(), is(false));

    PassiveReplicationMessage message3 = new PassiveReplicationMessage.ChainReplicationMessage(2, chain, 3L, 1L, clientId);
    passiveEntity.invokePassive(client.invokeContext(), message3);

    // Should be added as well, different message id
    assertThat(passiveEntity.getStateService().getStore(passiveEntity.getStoreIdentifier()).get(2).isEmpty(), is(false));
  }

  @Test
  public void testOversizeReplaceAtHeadMessage() throws Exception {
    ClusterTierPassiveEntity passiveEntity = new ClusterTierPassiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    passiveEntity.createNew();
    TestClientDescriptor client = TestClientDescriptor.newClient();

    int key = 2;

    Chain chain = sequencedChainOf(createPayload(1L));
    PassiveReplicationMessage message = new PassiveReplicationMessage.ChainReplicationMessage(key, chain, 2L, 1L, 3L);
    passiveEntity.invokePassive(client.invokeContext(), message);

    Chain oversizeChain = sequencedChainOf(createPayload(2L, 1024 * 1024));
    ServerStoreOpMessage.ReplaceAtHeadMessage oversizeMsg = new ServerStoreOpMessage.ReplaceAtHeadMessage(key, chain, oversizeChain);
    passiveEntity.invokePassive(client.invokeContext(), oversizeMsg);
    // Should be evicted, the value is oversize.
    assertThat(passiveEntity.getStateService().getStore(passiveEntity.getStoreIdentifier()).get(key).isEmpty(), is(true));
  }

  @Test
  public void testOversizeChainReplicationMessage() throws Exception {
    ClusterTierPassiveEntity passiveEntity = new ClusterTierPassiveEntity(defaultRegistry, defaultConfiguration, DEFAULT_MAPPER);
    passiveEntity.createNew();
    TestClientDescriptor client = TestClientDescriptor.newClient();

    long key = 2L;
    Chain oversizeChain = sequencedChainOf(createPayload(key, 1024 * 1024));
    PassiveReplicationMessage oversizeMsg = new PassiveReplicationMessage.ChainReplicationMessage(key, oversizeChain, 2L, 1L, (long) 3);
    passiveEntity.invokePassive(client.invokeContext(), oversizeMsg);
    // Should be cleared, the value is oversize.
    assertThat(passiveEntity.getStateService().getStore(passiveEntity.getStoreIdentifier()).get(key).isEmpty(), is(true));
  }

  /**
   * Builder for {@link ServerStoreConfiguration} instances.
   */
  private static final class ServerStoreConfigBuilder {
    private PoolAllocation poolAllocation;
    private String storedKeyType;
    private String storedValueType;
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
        keySerializerType, valueSerializerType, consistency, false, false);
    }
  }

  /**
   * Provides a {@link ServiceRegistry} for off-heap resources.  This is a "server-side" object.
   */
  private static final class OffHeapIdentifierRegistry implements ServiceRegistry {

    private final long offHeapSize;

    private EhcacheStateServiceImpl storeManagerService;

    private final Map<OffHeapResourceIdentifier, TestOffHeapResource> pools =
      new HashMap<>();

    private final Map<String, ServerSideConfiguration.Pool> sharedPools = new HashMap<>();

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

            @Override
            public boolean addOffHeapResource(OffHeapResourceIdentifier identifier, long capacity) {
              return false;
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
        return (T) mock(IEntityMessenger.class);
      } else if(serviceConfiguration instanceof EntityManagementRegistryConfiguration) {
        return null;
      } else if(serviceConfiguration instanceof BasicServiceConfiguration && serviceConfiguration.getServiceType() == IMonitoringProducer.class) {
        return null;
      } else if(serviceConfiguration instanceof OOOMessageHandlerConfiguration) {
        OOOMessageHandlerConfiguration<EntityMessage, EntityResponse> oooMessageHandlerConfiguration = (OOOMessageHandlerConfiguration) serviceConfiguration;
        return (T) new OOOMessageHandlerImpl<>(oooMessageHandlerConfiguration.getTrackerPolicy(), () -> {});
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
