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
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse.Failure;
import org.hamcrest.Matchers;
import org.junit.Test;
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

import static org.ehcache.clustered.common.messages.EhcacheEntityResponse.success;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 *
 * @author cdennis
 */
public class EhcacheActiveEntityTest {

  private static final byte[] ENTITY_ID = ClusteredEhcacheIdentity.serialize(UUID.randomUUID());

  @Test
  public void testConfigTooShort() {
    try {
      new EhcacheActiveEntity(null, new byte[15]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testConfigTooLong() {
    try {
      new EhcacheActiveEntity(null, new byte[17]);
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

  @Test
  public void testConnected() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(null, ENTITY_ID);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    final Map<ClientDescriptor, Set<String>> connectedClients = activeEntity.getConnectedClients();
    assertThat(connectedClients.keySet(), is(equalTo(Collections.singleton(client))));
    assertThat(connectedClients.get(client), is(Matchers.<String>empty()));

    final Map<String, Set<ClientDescriptor>> inUseStores = activeEntity.getInUseStores();
    assertThat(inUseStores.isEmpty(), is(true));
  }

  @Test
  public void testDisconnectedNotConnected() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(null, ENTITY_ID);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.disconnected(client);
    // Not expected to fail ...
  }

  @Test
  public void testDisconnected() throws Exception {
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(null, ENTITY_ID);

    ClientDescriptor client = new TestClientDescriptor();
    activeEntity.connected(client);

    activeEntity.disconnected(client);

    final Map<ClientDescriptor, Set<String>> connectedClients = activeEntity.getConnectedClients();
    assertThat(connectedClients.isEmpty(), is(true));

    final Map<String, Set<ClientDescriptor>> inUseStores = activeEntity.getInUseStores();
    assertThat(inUseStores.isEmpty(), is(true));
  }

  @Test
  public void testConfigure() throws Exception {
    final ServiceRegistry registry = new OffHeapIdentifierRegistry();
    final EhcacheActiveEntity activeEntity = new EhcacheActiveEntity(registry, ENTITY_ID);

    final Map<String, Pool> resourcePools = new HashMap<String, Pool>();
    resourcePools.put("primary", new Pool("serverResource1", MemoryUnit.MEGABYTES.toBytes(4L)));
    resourcePools.put("secondary", new Pool("serverResource2", MemoryUnit.MEGABYTES.toBytes(8L)));

    final EhcacheEntityMessage.ConfigureCacheManager configureMessage =
        EhcacheEntityMessage.configure(new ServerSideConfiguration("defaultServerResource", resourcePools));

    ClientDescriptor client = new TestClientDescriptor();
    assertSuccess(activeEntity.invoke(client, configureMessage));

    final Set<String> poolIds = activeEntity.getSharedResourcePoolIds();
    assertThat(poolIds, containsInAnyOrder("primary", "secondary"));
  }

  private void assertSuccess(EhcacheEntityResponse response) throws Exception {
    if (!response.equals(success())) {
      throw ((Failure)response).getCause();
    }
  }

  private static final class TestClientDescriptor implements ClientDescriptor {
  }

  private static final class OffHeapIdentifierRegistry implements ServiceRegistry {

    private final Map<OffHeapResourceIdentifier, OffHeapResource> pools =
        new HashMap<OffHeapResourceIdentifier, OffHeapResource>();

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getService(ServiceConfiguration<T> serviceConfiguration) {
      if (serviceConfiguration instanceof OffHeapResourceIdentifier) {
        final OffHeapResourceIdentifier resourceIdentifier = (OffHeapResourceIdentifier)serviceConfiguration;
        OffHeapResource offHeapResource = this.pools.get(resourceIdentifier);
        if (offHeapResource == null) {
          offHeapResource = new TestOffHeapResource(MemoryUnit.MEGABYTES.toBytes(32L));
          this.pools.put(resourceIdentifier, offHeapResource);
        }
        return (T)offHeapResource;    // unchecked
      }

      throw new UnsupportedOperationException("Registry.getService does not support " + serviceConfiguration.getClass().getName());
    }
  }

  private static final class TestOffHeapResource implements OffHeapResource {

    private long capacity;

    public TestOffHeapResource(long capacity) {
      this.capacity = capacity;
    }

    public long getCapacity() {
      return capacity;
    }

    @Override
    public boolean reserve(long size) throws IllegalArgumentException {
      if (size < 0) {
        throw new IllegalArgumentException();
      }
      if (size > this.capacity) {
        return false;
      } else {
        this.capacity -= size;
        return true;
      }
    }

    @Override
    public void release(long size) throws IllegalArgumentException {
      if (size < 0) {
        throw new IllegalArgumentException();
      }
      this.capacity += size;
    }

    @Override
    public long available() {
      return this.capacity;
    }
  }
}
