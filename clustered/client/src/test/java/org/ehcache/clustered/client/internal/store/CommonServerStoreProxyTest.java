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
package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityFactory;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService.PassthroughServerBuilder;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.ServerStoreMessageFactory;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.serialization.LongSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.terracotta.connection.Connection;

import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.ehcache.clustered.common.internal.store.Util.readPayLoad;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CommonServerStoreProxyTest {

  private static final String CACHE_IDENTIFIER = "testCache";
  private static final URI CLUSTER_URI = URI.create("terracotta://localhost");

  private static ClusterTierClientEntity clientEntity;
  private static CommonServerStoreProxy serverStoreProxy;

  @BeforeClass
  public static void setUp() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI,
        new PassthroughServerBuilder()
            .resource("defaultResource", 128, MemoryUnit.MB)
            .build());
    Connection connection = new UnitTestConnectionService().connect(CLUSTER_URI, new Properties());

    ClusterTierManagerClientEntityFactory entityFactory = new ClusterTierManagerClientEntityFactory(connection);

    ServerSideConfiguration serverConfig =
        new ServerSideConfiguration("defaultResource", Collections.<String, ServerSideConfiguration.Pool>emptyMap());
    entityFactory.create("TestCacheManager", serverConfig);

    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(16L, MemoryUnit.MB);

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(resourcePool.getPoolAllocation(), Long.class
      .getName(),
      Long.class.getName(), LongSerializer.class.getName(), LongSerializer.class
      .getName(), null);
    ClusterTierClientEntity clientEntity = entityFactory.fetchOrCreateClusteredStoreEntity(UUID.randomUUID(), "TestCacheManager", CACHE_IDENTIFIER, serverStoreConfiguration, true);
    clientEntity.validate(serverStoreConfiguration);
    serverStoreProxy = new CommonServerStoreProxy(CACHE_IDENTIFIER, new ServerStoreMessageFactory(clientEntity.getClientId()), clientEntity);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    serverStoreProxy = null;
    if (clientEntity != null) {
      clientEntity.close();
      clientEntity = null;
    }

    UnitTestConnectionService.remove(CLUSTER_URI);
  }

  @Test
  public void testGetKeyNotPresent() throws Exception {
    Chain chain = serverStoreProxy.get(1);

    assertThat(chain.isEmpty(), is(true));
  }

  @Test
  public void testGetAndAppendKeyNotPresent() throws Exception {
    Chain chain = serverStoreProxy.getAndAppend(4L, createPayload(4L));

    assertThat(chain.isEmpty(), is(true));

    chain = serverStoreProxy.get(4L);

    assertThat(chain.isEmpty(), is(false));
    assertChainHas(chain, 4L);
  }

  @Test
  public void testGetAndAppendMultipleTimesOnSameKey() throws Exception {
    serverStoreProxy.getAndAppend(5L, createPayload(5L));
    serverStoreProxy.getAndAppend(5L, createPayload(55L));
    serverStoreProxy.getAndAppend(5L, createPayload(555L));
    Chain chain = serverStoreProxy.getAndAppend(5l, createPayload(5555L));

    assertThat(chain.isEmpty(), is(false));
    assertChainHas(chain, 5L, 55L, 555L);
  }

  @Test
  public void testReplaceAtHeadSuccessFull() throws Exception {
    serverStoreProxy.getAndAppend(20L, createPayload(200L));
    serverStoreProxy.getAndAppend(20L, createPayload(2000L));
    serverStoreProxy.getAndAppend(20L, createPayload(20000L));

    Chain expect = serverStoreProxy.get(20L);
    Chain update = getChain(false, createPayload(400L));

    serverStoreProxy.replaceAtHead(20l, expect, update);

    Chain afterReplace = serverStoreProxy.get(20L);
    assertChainHas(afterReplace, 400L);

    serverStoreProxy.getAndAppend(20L, createPayload(4000L));
    serverStoreProxy.getAndAppend(20L, createPayload(40000L));

    serverStoreProxy.replaceAtHead(20L, afterReplace, getChain(false, createPayload(800L)));

    Chain anotherReplace = serverStoreProxy.get(20L);

    assertChainHas(anotherReplace, 800L, 4000L, 40000L);
  }

  @Test
  public void testClear() throws Exception {
    serverStoreProxy.getAndAppend(1L, createPayload(100L));

    serverStoreProxy.clear();
    Chain chain = serverStoreProxy.get(1);
    assertThat(chain.isEmpty(), is(true));
  }

  private static void assertChainHas(Chain chain, long... payLoads) {
    Iterator<Element> elements = chain.iterator();
    for (long payLoad : payLoads) {
      assertThat(readPayLoad(elements.next().getPayload()), is(Long.valueOf(payLoad)));
    }
    assertThat(elements.hasNext(), is(false));
  }

}
