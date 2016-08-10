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

import org.ehcache.clustered.client.TestTimeSource;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.client.internal.EhcacheClientEntityFactory;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.ServerStoreMessageFactory;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.Ehcache;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.spi.store.StoreAccessTimeoutException;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.expiry.Expirations;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.connection.Connection;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.util.StatisticsTestUtils.validateStat;
import static org.ehcache.clustered.util.StatisticsTestUtils.validateStats;
import static org.ehcache.expiry.Expirations.noExpiration;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusteredStoreTest {

  private static final String CACHE_IDENTIFIER = "testCache";
  private static final URI CLUSTER_URI = URI.create("terracotta://localhost:9510");

  ClusteredStore<Long, String> store;

  @Before
  public void setup() throws Exception {
    UnitTestConnectionService.add(
        CLUSTER_URI,
        new UnitTestConnectionService.PassthroughServerBuilder().resource("defaultResource", 8, MemoryUnit.MB).build()
    );

    Connection connection = new UnitTestConnectionService().connect(CLUSTER_URI, new Properties());
    EhcacheClientEntityFactory entityFactory = new EhcacheClientEntityFactory(connection);

    ServerSideConfiguration serverConfig =
        new ServerSideConfiguration("defaultResource", Collections.<String, ServerSideConfiguration.Pool>emptyMap());
    entityFactory.create("TestCacheManager", serverConfig);

    EhcacheClientEntity clientEntity = entityFactory.retrieve("TestCacheManager", serverConfig);
    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB);
    ServerStoreConfiguration serverStoreConfiguration =
        new ServerStoreConfiguration(resourcePool.getPoolAllocation(),
            Long.class.getName(), String.class.getName(),
            Long.class.getName(), String.class.getName(),
            LongSerializer.class.getName(), StringSerializer.class.getName(),
            null
    );
    clientEntity.createCache(CACHE_IDENTIFIER, serverStoreConfiguration);
    ServerStoreMessageFactory factory = new ServerStoreMessageFactory(CACHE_IDENTIFIER);
    ServerStoreProxy serverStoreProxy = new NoInvalidationServerStoreProxy(factory, clientEntity);

    TestTimeSource testTimeSource = new TestTimeSource();

    OperationsCodec<Long, String> codec = new OperationsCodec<Long, String>(new LongSerializer(), new StringSerializer());
    ChainResolver<Long, String> resolver = new ChainResolver<Long, String>(codec, Expirations.noExpiration());
    store = new ClusteredStore<Long, String>(codec, resolver, serverStoreProxy, testTimeSource);
  }

  @After
  public void tearDown() throws Exception {
    UnitTestConnectionService.remove("terracotta://localhost:9510/my-application?auto-create");
  }

  @Test
  public void testPut() throws Exception {
    assertThat(store.put(1L, "one"), is(Store.PutStatus.PUT));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.PutOutcome.PUT));
    assertThat(store.put(1L, "another one"), is(Store.PutStatus.UPDATE));
    assertThat(store.put(1L, "yet another one"), is(Store.PutStatus.UPDATE));
    validateStat(store, StoreOperationOutcomes.PutOutcome.REPLACED, 2);
    validateStat(store, StoreOperationOutcomes.PutOutcome.PUT, 1);
  }

  @Test(expected = StoreAccessTimeoutException.class)
  public void testPutTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    when(proxy.getAndAppend(anyLong(), any(ByteBuffer.class))).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, null, proxy, timeSource);
    store.put(1L, "one");
  }

  @Test
  public void testGet() throws Exception {
    assertThat(store.get(1L), nullValue());
    validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.MISS));
    store.put(1L, "one");
    assertThat(store.get(1L).value(), is("one"));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.MISS, StoreOperationOutcomes.GetOutcome.HIT));
  }

  @Test(expected = StoreAccessException.class)
  public void testGetThrowsOnlySAE() throws Exception {
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    ChainResolver chainResolver = mock(ChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.get(anyLong())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, chainResolver, serverStoreProxy, testTimeSource);
    store.get(1L);
  }

  @Test
  public void testGetTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    when(proxy.get(1L)).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(null, null, proxy, null);
    assertThat(store.get(1L), nullValue());
    validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.TIMEOUT));
  }

  @Test
  public void testGetThatCompactsInvokesReplace() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    timeSource.advanceTime(134556L);
    long now = timeSource.getTimeMillis();
    OperationsCodec<Long, String> operationsCodec = new OperationsCodec<Long, String>(new LongSerializer(), new StringSerializer());
    ChainResolver chainResolver = mock(ChainResolver.class);
    ResolvedChain<Long, String> resolvedChain = mock(ResolvedChain.class);
    when(resolvedChain.isCompacted()).thenReturn(true);
    when(chainResolver.resolve(any(Chain.class), eq(42L), eq(now))).thenReturn(resolvedChain);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    Chain chain = mock(Chain.class);
    when(chain.isEmpty()).thenReturn(false);
    when(serverStoreProxy.get(42L)).thenReturn(chain);

    ClusteredStore<Long, String> clusteredStore = new ClusteredStore<Long, String>(operationsCodec, chainResolver,
                                                                                    serverStoreProxy, timeSource);
    clusteredStore.get(42L);
    verify(serverStoreProxy).replaceAtHead(eq(42L), eq(chain), any(Chain.class));
  }

  @Test
  public void testGetThatDoesNotCompactsInvokesReplace() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    timeSource.advanceTime(134556L);
    long now = timeSource.getTimeMillis();
    OperationsCodec<Long, String> operationsCodec = new OperationsCodec<Long, String>(new LongSerializer(), new StringSerializer());
    ChainResolver chainResolver = mock(ChainResolver.class);
    ResolvedChain<Long, String> resolvedChain = mock(ResolvedChain.class);
    when(resolvedChain.isCompacted()).thenReturn(false);
    when(chainResolver.resolve(any(Chain.class), eq(42L), eq(now))).thenReturn(resolvedChain);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    Chain chain = mock(Chain.class);
    when(chain.isEmpty()).thenReturn(false);
    when(serverStoreProxy.get(42L)).thenReturn(chain);

    ClusteredStore<Long, String> clusteredStore = new ClusteredStore<Long, String>(operationsCodec, chainResolver,
                                                                                    serverStoreProxy, timeSource);
    clusteredStore.get(42L);
    verify(serverStoreProxy, never()).replaceAtHead(eq(42L), eq(chain), any(Chain.class));
  }

  @Test
  public void testContainsKey() throws Exception {
    assertThat(store.containsKey(1L), is(false));
    store.put(1L, "one");
    assertThat(store.containsKey(1L), is(true));
    validateStat(store, StoreOperationOutcomes.GetOutcome.HIT, 0);
    validateStat(store, StoreOperationOutcomes.GetOutcome.MISS, 0);
  }

  @Test(expected = StoreAccessException.class)
  public void testContainsKeyThrowsOnlySAE() throws Exception {
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    ChainResolver chainResolver = mock(ChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.get(anyLong())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, chainResolver, serverStoreProxy, testTimeSource);
    store.containsKey(1L);
  }

  @Test
  public void testRemove() throws Exception {
    assertThat(store.remove(1L), is(false));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.RemoveOutcome.MISS));
    store.put(1L, "one");
    assertThat(store.remove(1L), is(true));
    assertThat(store.containsKey(1L), is(false));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.RemoveOutcome.MISS, StoreOperationOutcomes.RemoveOutcome.REMOVED));
  }

  @Test(expected = StoreAccessException.class)
  public void testRemoveThrowsOnlySAE() throws Exception {
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    ChainResolver chainResolver = mock(ChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.get(anyLong())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, chainResolver, serverStoreProxy, testTimeSource);
    store.remove(1L);
  }

  @Test(expected = StoreAccessTimeoutException.class)
  public void testRemoveTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    when(proxy.getAndAppend(anyLong(), any(ByteBuffer.class))).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, null, proxy, timeSource);
    store.remove(1L);
  }

  @Test
  public void testClear() throws Exception {
    assertThat(store.containsKey(1L), is(false));
    store.clear();
    assertThat(store.containsKey(1L), is(false));

    store.put(1L, "one");
    store.put(2L, "two");
    store.put(3L, "three");
    assertThat(store.containsKey(1L), is(true));

    store.clear();

    assertThat(store.containsKey(1L), is(false));
    assertThat(store.containsKey(2L), is(false));
    assertThat(store.containsKey(3L), is(false));
  }

  @Test(expected = StoreAccessException.class)
  public void testClearThrowsOnlySAE() throws Exception {
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    ChainResolver chainResolver = mock(ChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    doThrow(new RuntimeException()).when(serverStoreProxy).clear();
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, chainResolver, serverStoreProxy, testTimeSource);
    store.clear();
  }

  @Test(expected = StoreAccessTimeoutException.class)
  public void testClearTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    doThrow(TimeoutException.class).when(proxy).clear();
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, null, proxy, timeSource);
    store.clear();
  }

  @Test
  public void testPutIfAbsent() throws Exception {
    assertThat(store.putIfAbsent(1L, "one"), nullValue());
    validateStats(store, EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.PUT));
    assertThat(store.putIfAbsent(1L, "another one").value(), is("one"));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.PUT, StoreOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  @Test(expected = StoreAccessException.class)
  public void testPutIfAbsentThrowsOnlySAE() throws Exception {
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    ChainResolver chainResolver = mock(ChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.get(anyLong())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, chainResolver, serverStoreProxy, testTimeSource);
    store.putIfAbsent(1L, "one");
  }

  @Test(expected = StoreAccessTimeoutException.class)
  public void testPutIfAbsentTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    when(proxy.getAndAppend(anyLong(), any(ByteBuffer.class))).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, null, proxy, timeSource);
    store.putIfAbsent(1L, "one");
  }

  @Test
  public void testConditionalRemove() throws Exception {
    assertThat(store.remove(1L, "one"), is(Store.RemoveStatus.KEY_MISSING));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS));
    store.put(1L, "one");
    assertThat(store.remove(1L, "one"), is(Store.RemoveStatus.REMOVED));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS, StoreOperationOutcomes.ConditionalRemoveOutcome.REMOVED));
    store.put(1L, "another one");
    assertThat(store.remove(1L, "one"), is(Store.RemoveStatus.KEY_PRESENT));
    validateStat(store, StoreOperationOutcomes.ConditionalRemoveOutcome.MISS, 2);
  }

  @Test(expected = StoreAccessException.class)
  public void testConditionalRemoveThrowsOnlySAE() throws Exception {
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    ChainResolver chainResolver = mock(ChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.get(anyLong())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, chainResolver, serverStoreProxy, testTimeSource);
    store.remove(1L, "one");
  }

  @Test(expected = StoreAccessTimeoutException.class)
  public void testConditionalRemoveTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    when(proxy.getAndAppend(anyLong(), any(ByteBuffer.class))).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, null, proxy, timeSource);
    store.remove(1L, "one");
  }

  @Test
  public void testReplace() throws Exception {
    assertThat(store.replace(1L, "one"), nullValue());
    validateStats(store, EnumSet.of(StoreOperationOutcomes.ReplaceOutcome.MISS));
    store.put(1L, "one");
    assertThat(store.replace(1L, "another one").value(), is("one"));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.ReplaceOutcome.MISS, StoreOperationOutcomes.ReplaceOutcome.REPLACED));
  }

  @Test(expected = StoreAccessException.class)
  public void testReplaceThrowsOnlySAE() throws Exception {
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    ChainResolver chainResolver = mock(ChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.get(anyLong())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, chainResolver, serverStoreProxy, testTimeSource);
    store.replace(1L, "one");
  }

  @Test(expected = StoreAccessTimeoutException.class)
  public void testReplaceTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    when(proxy.getAndAppend(anyLong(), any(ByteBuffer.class))).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, null, proxy, timeSource);
    store.replace(1L, "one");
  }

  @Test
  public void testConditionalReplace() throws Exception {
    assertThat(store.replace(1L, "one" , "another one"), is(Store.ReplaceStatus.MISS_NOT_PRESENT));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalReplaceOutcome.MISS));
    store.put(1L, "some other one");
    assertThat(store.replace(1L, "one" , "another one"), is(Store.ReplaceStatus.MISS_PRESENT));
    validateStat(store, StoreOperationOutcomes.ConditionalReplaceOutcome.MISS, 2);
    validateStat(store, StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED, 0);
    assertThat(store.replace(1L, "some other one" , "another one"), is(Store.ReplaceStatus.HIT));
    validateStat(store, StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED, 1);
    validateStat(store, StoreOperationOutcomes.ConditionalReplaceOutcome.MISS, 2);
  }

  @Test(expected = StoreAccessException.class)
  public void testConditionalReplaceThrowsOnlySAE() throws Exception {
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    ChainResolver chainResolver = mock(ChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.get(anyLong())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, chainResolver, serverStoreProxy, testTimeSource);
    store.replace(1L, "one", "another one");
  }

  @Test(expected = StoreAccessTimeoutException.class)
  public void testConditionalReplaceTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    when(proxy.getAndAppend(anyLong(), any(ByteBuffer.class))).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<Long, String>(codec, null, proxy, timeSource);
    store.replace(1L, "one", "another one");
  }

  @Test
  public void testBulkComputePutAll() throws Exception {
    store.put(1L, "another one");
    Map<Long, String> map =  new HashMap<Long, String>();
    map.put(1L, "one");
    map.put(2L, "two");
    Ehcache.PutAllFunction<Long, String> putAllFunction = new Ehcache.PutAllFunction<Long, String>(null, map, null);
    Map<Long, Store.ValueHolder<String>> valueHolderMap = store.bulkCompute(new HashSet<Long>(Arrays.asList(1L, 2L)), putAllFunction);

    assertThat(valueHolderMap.get(1L).value(), is(map.get(1L)));
    assertThat(store.get(1L).value(), is(map.get(1L)));
    assertThat(valueHolderMap.get(2L).value(), is(map.get(2L)));
    assertThat(store.get(2L).value(), is(map.get(2L)));
    assertThat(putAllFunction.getActualPutCount().get(), is(2));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.PutOutcome.PUT));  //outcome of the initial store put
  }

  @Test
  public void testBulkComputeRemoveAll() throws Exception {
    store.put(1L, "one");
    store.put(2L, "two");
    store.put(3L, "three");
    Ehcache.RemoveAllFunction<Long, String> removeAllFunction = new Ehcache.RemoveAllFunction<Long, String>();
    Map<Long, Store.ValueHolder<String>> valueHolderMap = store.bulkCompute(new HashSet<Long>(Arrays.asList(1L, 2L, 4L)), removeAllFunction);

    assertThat(valueHolderMap.get(1L), nullValue());
    assertThat(store.get(1L), nullValue());
    assertThat(valueHolderMap.get(2L), nullValue());
    assertThat(store.get(2L), nullValue());
    assertThat(valueHolderMap.get(4L), nullValue());
    assertThat(store.get(4L), nullValue());
    validateStats(store, EnumSet.noneOf(StoreOperationOutcomes.RemoveOutcome.class));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testBulkComputeThrowsForGenericFunction() throws Exception {
    Function<Iterable<? extends Map.Entry<? extends Long, ? extends String>>, Iterable<? extends Map.Entry<? extends Long, ? extends String>>> remappingFunction
        = mock(Function.class);
    store.bulkCompute(new HashSet<Long>(Arrays.asList(1L, 2L)), remappingFunction);
  }

  @Test
  public void testBulkComputeIfAbsentGetAll() throws Exception {
    store.put(1L, "one");
    store.put(2L, "two");
    Ehcache.GetAllFunction<Long, String> getAllAllFunction = new Ehcache.GetAllFunction<Long, String>();
    Map<Long, Store.ValueHolder<String>> valueHolderMap = store.bulkComputeIfAbsent(new HashSet<Long>(Arrays.asList(1L, 2L)), getAllAllFunction);

    assertThat(valueHolderMap.get(1L).value(), is("one"));
    assertThat(store.get(1L).value(), is("one"));
    assertThat(valueHolderMap.get(2L).value(), is("two"));
    assertThat(store.get(2L).value(), is("two"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testBulkComputeIfAbsentThrowsForGenericFunction() throws Exception {
    Function<Iterable<? extends Long>, Iterable<? extends Map.Entry<? extends Long, ? extends String>>> mappingFunction
        = mock(Function.class);
    store.bulkComputeIfAbsent(new HashSet<Long>(Arrays.asList(1L, 2L)), mappingFunction);
  }

}