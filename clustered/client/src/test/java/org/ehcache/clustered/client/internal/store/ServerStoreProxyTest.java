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
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.client.internal.EhcacheClientEntityFactory;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.Element;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.serialization.LongSerializer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.passthrough.PassthroughServer;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

public class ServerStoreProxyTest {


  private static final String CACHE_IDENTIFIER = "testCache";

  private static ServerStoreProxy serverStoreProxy;

  @BeforeClass
  public static void setUp() throws Exception {
    PassthroughServer server = UnitTestConnectionService.server();

    Connection connection = server.connectNewClient();

    EhcacheClientEntityFactory entityFactory = new EhcacheClientEntityFactory(connection);

    EhcacheClientEntity clientEntity = entityFactory.create("TestCacheManager",
        new ServerSideConfiguration("defaultResource", Collections.EMPTY_MAP));

    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.fixed(16L, MemoryUnit.MB);

    clientEntity.createCache(CACHE_IDENTIFIER, new ServerStoreConfiguration(resourcePool.getPoolAllocation(), Long.class.getName(),
        Long.class.getName(), Long.class.getName(), Long.class.getName(), LongSerializer.class.getName(), LongSerializer.class
        .getName()));
    serverStoreProxy = new ServerStoreProxy(CACHE_IDENTIFIER, clientEntity);
  }



  @Test
  public void testGetKeyNotPresent() throws Exception {
    Chain chain = serverStoreProxy.get(1);

    assertThat(chain.isEmpty(), is(true));
  }

  @Test
  public void testAppendKeyNotPresent() throws Exception {
    serverStoreProxy.append(2, createPayload(2));

    Chain chain = serverStoreProxy.get(2);
    assertThat(chain.isEmpty(), is(false));
    assertThat(readPayLoad(chain.iterator().next().getPayload()), is(2L));
  }

  @Test
  public void testGetAfterMultipleAppendsOnSameKey() throws Exception {

    serverStoreProxy.append(3L, createPayload(3L));
    serverStoreProxy.append(3L, createPayload(33L));
    serverStoreProxy.append(3L, createPayload(333L));

    Chain chain = serverStoreProxy.get(3L);

    assertThat(chain.isEmpty(), is(false));

    assertChainHas(chain, 3L, 33L, 333l);
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



  private static long readPayLoad(ByteBuffer byteBuffer) {
    return byteBuffer.getLong();
  }

  private static ByteBuffer createPayload(long key) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8).putLong(key);
    byteBuffer.flip();
    return byteBuffer.asReadOnlyBuffer();
  }

  private static void assertChainHas(Chain chain, long... payLoads) {
    Iterator<Element> elements = chain.iterator();
    for (long payLoad : payLoads) {
      assertThat(readPayLoad(elements.next().getPayload()), is(Long.valueOf(payLoad)));
    }
    assertThat(elements.hasNext(), is(false));
  }

}
