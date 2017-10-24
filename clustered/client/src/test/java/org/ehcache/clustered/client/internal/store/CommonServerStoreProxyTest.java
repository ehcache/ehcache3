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

import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.ehcache.clustered.common.internal.store.Util.readPayLoad;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CommonServerStoreProxyTest extends AbstractServerStoreProxyTest {

  private static ClusterTierClientEntity createClientEntity(String name) throws Exception {
    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(16L, MemoryUnit.MB);

    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(resourcePool.getPoolAllocation(), Long.class
      .getName(),
      Long.class.getName(), LongSerializer.class.getName(), LongSerializer.class
      .getName(), null);

    return createClientEntity(name, serverStoreConfiguration, true);

  }

  @Test
  public void testGetKeyNotPresent() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testGetKeyNotPresent");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testGetKeyNotPresent", new ServerStoreMessageFactory(), clientEntity, null);

    Chain chain = serverStoreProxy.get(1);

    assertThat(chain.isEmpty(), is(true));
  }

  @Test
  public void testAppendKeyNotPresent() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testAppendKeyNotPresent");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testAppendKeyNotPresent", new ServerStoreMessageFactory(), clientEntity, null);

    serverStoreProxy.append(2, createPayload(2));

    Chain chain = serverStoreProxy.get(2);
    assertThat(chain.isEmpty(), is(false));
    assertThat(readPayLoad(chain.iterator().next().getPayload()), is(2L));
  }

  @Test
  public void testGetAfterMultipleAppendsOnSameKey() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testGetAfterMultipleAppendsOnSameKey");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testGetAfterMultipleAppendsOnSameKey", new ServerStoreMessageFactory(), clientEntity, null);

    serverStoreProxy.append(3L, createPayload(3L));
    serverStoreProxy.append(3L, createPayload(33L));
    serverStoreProxy.append(3L, createPayload(333L));

    Chain chain = serverStoreProxy.get(3L);

    assertThat(chain.isEmpty(), is(false));

    assertChainHas(chain, 3L, 33L, 333l);
  }

  @Test
  public void testGetAndAppendKeyNotPresent() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testGetAndAppendKeyNotPresent");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testGetAndAppendKeyNotPresent", new ServerStoreMessageFactory(), clientEntity, null);
    Chain chain = serverStoreProxy.getAndAppend(4L, createPayload(4L));

    assertThat(chain.isEmpty(), is(true));

    chain = serverStoreProxy.get(4L);

    assertThat(chain.isEmpty(), is(false));
    assertChainHas(chain, 4L);
  }

  @Test
  public void testGetAndAppendMultipleTimesOnSameKey() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testGetAndAppendMultipleTimesOnSameKey");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testGetAndAppendMultipleTimesOnSameKey", new ServerStoreMessageFactory(), clientEntity, null);
    serverStoreProxy.getAndAppend(5L, createPayload(5L));
    serverStoreProxy.getAndAppend(5L, createPayload(55L));
    serverStoreProxy.getAndAppend(5L, createPayload(555L));
    Chain chain = serverStoreProxy.getAndAppend(5l, createPayload(5555L));

    assertThat(chain.isEmpty(), is(false));
    assertChainHas(chain, 5L, 55L, 555L);
  }

  @Test
  public void testReplaceAtHeadSuccessFull() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testReplaceAtHeadSuccessFull");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testReplaceAtHeadSuccessFull", new ServerStoreMessageFactory(), clientEntity, null);
    serverStoreProxy.append(20L, createPayload(200L));
    serverStoreProxy.append(20L, createPayload(2000L));
    serverStoreProxy.append(20L, createPayload(20000L));

    Chain expect = serverStoreProxy.get(20L);
    Chain update = getChain(false, createPayload(400L));

    serverStoreProxy.replaceAtHead(20l, expect, update);

    Chain afterReplace = serverStoreProxy.get(20L);
    assertChainHas(afterReplace, 400L);

    serverStoreProxy.append(20L, createPayload(4000L));
    serverStoreProxy.append(20L, createPayload(40000L));

    serverStoreProxy.replaceAtHead(20L, afterReplace, getChain(false, createPayload(800L)));

    Chain anotherReplace = serverStoreProxy.get(20L);

    assertChainHas(anotherReplace, 800L, 4000L, 40000L);
  }

  @Test
  public void testClear() throws Exception {
    ClusterTierClientEntity clientEntity = createClientEntity("testClear");
    CommonServerStoreProxy serverStoreProxy = new CommonServerStoreProxy("testClear", new ServerStoreMessageFactory(), clientEntity, null);
    serverStoreProxy.append(1L, createPayload(100L));

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
