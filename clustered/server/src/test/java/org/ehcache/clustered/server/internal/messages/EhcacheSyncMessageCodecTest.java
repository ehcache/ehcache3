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
package org.ehcache.clustered.server.internal.messages;

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.CommonConfigCodec;
import org.ehcache.clustered.common.internal.store.Chain;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.ehcache.clustered.common.internal.store.Util.chainsEqual;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class EhcacheSyncMessageCodecTest {

  @Test
  public void testStateSyncMessageEncodeDecode() throws Exception {
    Map<String, ServerSideConfiguration.Pool> sharedPools = new HashMap<>();
    ServerSideConfiguration.Pool pool1 = new ServerSideConfiguration.Pool(1, "foo1");
    ServerSideConfiguration.Pool pool2 = new ServerSideConfiguration.Pool(2, "foo2");
    sharedPools.put("shared-pool-1", pool1);
    sharedPools.put("shared-pool-2", pool2);
    ServerSideConfiguration serverSideConfig = new ServerSideConfiguration("default-pool", sharedPools);

    PoolAllocation poolAllocation1 = new PoolAllocation.Dedicated("dedicated", 4);
    ServerStoreConfiguration serverStoreConfiguration1 = new ServerStoreConfiguration(poolAllocation1,
      "storedKeyType1", "storedValueType1", null, null,
      "keySerializerType1", "valueSerializerType1", Consistency.STRONG);

    PoolAllocation poolAllocation2 = new PoolAllocation.Shared("shared");
    ServerStoreConfiguration serverStoreConfiguration2 = new ServerStoreConfiguration(poolAllocation2,
      "storedKeyType2", "storedValueType2", null, null,
      "keySerializerType2", "valueSerializerType2", Consistency.EVENTUAL);

    Map<String, ServerStoreConfiguration> storeConfigs = new HashMap<>();
    storeConfigs.put("cache1", serverStoreConfiguration1);
    storeConfigs.put("cache2", serverStoreConfiguration2);

    EhcacheStateSyncMessage message = new EhcacheStateSyncMessage(serverSideConfig, storeConfigs);
    EhcacheSyncMessageCodec codec = new EhcacheSyncMessageCodec(new CommonConfigCodec());
    EhcacheStateSyncMessage decodedMessage = (EhcacheStateSyncMessage) codec.decode(0, codec.encode(0, message));

    assertThat(decodedMessage.getConfiguration().getDefaultServerResource(), is("default-pool"));
    assertThat(decodedMessage.getConfiguration().getResourcePools(), is(sharedPools));
    assertThat(decodedMessage.getStoreConfigs().keySet(), containsInAnyOrder("cache1", "cache2"));

    ServerStoreConfiguration serverStoreConfiguration = decodedMessage.getStoreConfigs().get("cache1");
    assertThat(serverStoreConfiguration.getPoolAllocation(), instanceOf(PoolAllocation.Dedicated.class));
    PoolAllocation.Dedicated dedicatedPool = (PoolAllocation.Dedicated) serverStoreConfiguration.getPoolAllocation();
    assertThat(dedicatedPool.getResourceName(), is("dedicated"));
    assertThat(dedicatedPool.getSize(), is(4L));
    assertThat(serverStoreConfiguration.getStoredKeyType(), is("storedKeyType1"));
    assertThat(serverStoreConfiguration.getStoredValueType(), is("storedValueType1"));
    assertThat(serverStoreConfiguration.getKeySerializerType(), is("keySerializerType1"));
    assertThat(serverStoreConfiguration.getValueSerializerType(), is("valueSerializerType1"));
    assertThat(serverStoreConfiguration.getConsistency(), is(Consistency.STRONG));

    serverStoreConfiguration = decodedMessage.getStoreConfigs().get("cache2");
    assertThat(serverStoreConfiguration.getPoolAllocation(), instanceOf(PoolAllocation.Shared.class));
    PoolAllocation.Shared sharedPool = (PoolAllocation.Shared) serverStoreConfiguration.getPoolAllocation();
    assertThat(sharedPool.getResourcePoolName(), is("shared"));
    assertThat(serverStoreConfiguration.getStoredKeyType(), is("storedKeyType2"));
    assertThat(serverStoreConfiguration.getStoredValueType(), is("storedValueType2"));
    assertThat(serverStoreConfiguration.getKeySerializerType(), is("keySerializerType2"));
    assertThat(serverStoreConfiguration.getValueSerializerType(), is("valueSerializerType2"));
    assertThat(serverStoreConfiguration.getConsistency(), is(Consistency.EVENTUAL));
  }

  @Test
  public void testDataSyncMessageEncodeDecode() throws Exception {
    EhcacheSyncMessageCodec codec = new EhcacheSyncMessageCodec(new CommonConfigCodec());
    Map<Long, Chain> chainMap = new HashMap<>();
    Chain chain = getChain(true, createPayload(10L), createPayload(100L), createPayload(1000L));
    chainMap.put(1L, chain);
    chainMap.put(2L, chain);
    chainMap.put(3L, chain);
    EhcacheDataSyncMessage message = new EhcacheDataSyncMessage("foo", chainMap);
    byte[] encodedMessage = codec.encode(0, message);
    EhcacheDataSyncMessage decoded = (EhcacheDataSyncMessage) codec.decode(0, encodedMessage);
    assertThat(decoded.getCacheId(), is(message.getCacheId()));
    Map<Long, Chain> decodedChainMap = decoded.getChainMap();
    assertThat(decodedChainMap.size(), is(3));
    assertThat(chainsEqual(decodedChainMap.get(1L), chain), is(true));
    assertThat(chainsEqual(decodedChainMap.get(2L), chain), is(true));
    assertThat(chainsEqual(decodedChainMap.get(3L), chain), is(true));
  }
}
