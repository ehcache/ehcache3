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

import com.google.common.base.Objects;
import org.assertj.core.api.ThrowableAssert;
import org.ehcache.Cache;
import org.ehcache.clustered.client.TestTimeSource;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityFactory;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.ServerCallback;
import org.ehcache.clustered.client.internal.store.operations.EternalChainResolver;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.Ehcache;
import org.ehcache.core.spi.store.Store;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.impl.store.HashUtils;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.spi.serialization.Serializer;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.connection.Connection;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.ehcache.clustered.util.StatisticsTestUtils.validateStat;
import static org.ehcache.clustered.util.StatisticsTestUtils.validateStats;
import static org.ehcache.core.spi.store.Store.ValueHolder.NO_EXPIRE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.CombinableMatcher.either;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class ClusteredStoreTest {

  private static final String CACHE_IDENTIFIER = "testCache";
  private static final URI CLUSTER_URI = URI.create("terracotta://localhost");

  private ClusteredStore<Long, String> store;

  private final Store.Configuration<Long, String> config = new Store.Configuration<Long, String>() {

    @Override
    public Class<Long> getKeyType() {
      return Long.class;
    }

    @Override
    public Class<String> getValueType() {
      return String.class;
    }

    @Override
    public EvictionAdvisor<? super Long, ? super String> getEvictionAdvisor() {
      return null;
    }

    @Override
    public ClassLoader getClassLoader() {
      return null;
    }

    @Override
    public ExpiryPolicy<? super Long, ? super String> getExpiry() {
      return null;
    }

    @Override
    public ResourcePools getResourcePools() {
      return null;
    }

    @Override
    public Serializer<Long> getKeySerializer() {
      return null;
    }

    @Override
    public Serializer<String> getValueSerializer() {
      return null;
    }

    @Override
    public int getDispatcherConcurrency() {
      return 0;
    }

    @Override
    public CacheLoaderWriter<? super Long, String> getCacheLoaderWriter() {
      return null;
    }
  };

  @Before
  public void setup() throws Exception {
    UnitTestConnectionService.add(
        CLUSTER_URI,
        new UnitTestConnectionService.PassthroughServerBuilder().resource("defaultResource", 8, MemoryUnit.MB).build()
    );

    Connection connection = new UnitTestConnectionService().connect(CLUSTER_URI, new Properties());
    ClusterTierManagerClientEntityFactory entityFactory = new ClusterTierManagerClientEntityFactory(connection);

    ServerSideConfiguration serverConfig =
        new ServerSideConfiguration("defaultResource", Collections.emptyMap());
    entityFactory.create("TestCacheManager", serverConfig);

    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(4, MemoryUnit.MB);
    ServerStoreConfiguration serverStoreConfiguration = new ServerStoreConfiguration(resourcePool.getPoolAllocation(),
      Long.class.getName(), String.class.getName(), LongSerializer.class.getName(), StringSerializer.class.getName(), null, false);
    ClusterTierClientEntity clientEntity = entityFactory.fetchOrCreateClusteredStoreEntity("TestCacheManager", CACHE_IDENTIFIER, serverStoreConfiguration, true);
    clientEntity.validate(serverStoreConfiguration);
    ServerStoreProxy serverStoreProxy = new CommonServerStoreProxy(CACHE_IDENTIFIER, clientEntity, mock(ServerCallback.class));

    TestTimeSource testTimeSource = new TestTimeSource();

    OperationsCodec<Long, String> codec = new OperationsCodec<>(new LongSerializer(), new StringSerializer());
    EternalChainResolver<Long, String> resolver = new EternalChainResolver<>(codec);
    store = new ClusteredStore<>(config, codec, resolver, serverStoreProxy, testTimeSource);
  }

  @After
  public void tearDown() throws Exception {
    UnitTestConnectionService.remove("terracotta://localhost/my-application");
  }

  private void assertTimeoutOccurred(ThrowableAssert.ThrowingCallable throwingCallable) {
    assertThatExceptionOfType(StoreAccessException.class)
      .isThrownBy(throwingCallable)
      .withCauseInstanceOf(TimeoutException.class);
  }

  @Test
  public void testPut() throws Exception {
    assertThat(store.put(1L, "one"), is(Store.PutStatus.PUT));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.PutOutcome.PUT));
    assertThat(store.put(1L, "another one"), is(Store.PutStatus.PUT));
    assertThat(store.put(1L, "yet another one"), is(Store.PutStatus.PUT));
    validateStat(store, StoreOperationOutcomes.PutOutcome.PUT, 3);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPutTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    doThrow(TimeoutException.class).when(proxy).append(anyLong(), isNull());
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, null, proxy, timeSource);

    assertTimeoutOccurred(() -> store.put(1L, "one"));
  }

  @Test
  public void testGet() throws Exception {
    assertThat(store.get(1L), nullValue());
    validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.MISS));
    store.put(1L, "one");
    assertThat(store.get(1L).get(), is("one"));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.MISS, StoreOperationOutcomes.GetOutcome.HIT));
  }

  @Test(expected = StoreAccessException.class)
  public void testGetThrowsOnlySAE() throws Exception {
    @SuppressWarnings("unchecked")
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    @SuppressWarnings("unchecked")
    EternalChainResolver<Long, String> chainResolver = mock(EternalChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.get(anyLong())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, chainResolver, serverStoreProxy, testTimeSource);
    store.get(1L);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    long longKey = HashUtils.intHashToLong(new Long(1L).hashCode());
    when(proxy.get(longKey)).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config,null, null, proxy, null);
    assertThat(store.get(1L), nullValue());
    validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.TIMEOUT));
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
    @SuppressWarnings("unchecked")
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    @SuppressWarnings("unchecked")
    EternalChainResolver<Long, String> chainResolver = mock(EternalChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.get(anyLong())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, chainResolver, serverStoreProxy, testTimeSource);
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

  @Test
  public void testRemoveThrowsOnlySAE() throws Exception {
    @SuppressWarnings("unchecked")
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    @SuppressWarnings("unchecked")
    EternalChainResolver<Long, String> chainResolver = mock(EternalChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    RuntimeException theException = new RuntimeException();
    when(serverStoreProxy.getAndAppend(anyLong(), any())).thenThrow(theException);
    TestTimeSource testTimeSource = new TestTimeSource();

    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, chainResolver, serverStoreProxy, testTimeSource);
    assertThatExceptionOfType(StoreAccessException.class)
      .isThrownBy(() -> store.remove(1L))
      .withCause(theException);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRemoveTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    when(proxy.getAndAppend(anyLong(), isNull())).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, null, proxy, timeSource);

    assertTimeoutOccurred(() -> store.remove(1L));
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
    @SuppressWarnings("unchecked")
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    @SuppressWarnings("unchecked")
    EternalChainResolver<Long, String> chainResolver = mock(EternalChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    doThrow(new RuntimeException()).when(serverStoreProxy).clear();
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, chainResolver, serverStoreProxy, testTimeSource);
    store.clear();
  }

  @Test
  public void testClearTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    @SuppressWarnings("unchecked")
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    doThrow(TimeoutException.class).when(proxy).clear();
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, null, proxy, timeSource);

    assertTimeoutOccurred(() -> store.clear());
  }

  @Test
  public void testPutIfAbsent() throws Exception {
    assertThat(store.putIfAbsent(1L, "one", b -> {}), nullValue());
    validateStats(store, EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.PUT));
    assertThat(store.putIfAbsent(1L, "another one", b -> {}).get(), is("one"));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.PUT, StoreOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  @Test(expected = StoreAccessException.class)
  public void testPutIfAbsentThrowsOnlySAE() throws Exception {
    @SuppressWarnings("unchecked")
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    @SuppressWarnings("unchecked")
    EternalChainResolver<Long, String> chainResolver = mock(EternalChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.getAndAppend(anyLong(), any())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, chainResolver, serverStoreProxy, testTimeSource);
    store.putIfAbsent(1L, "one", b -> {});
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPutIfAbsentTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    when(proxy.getAndAppend(anyLong(), isNull())).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, null, proxy, timeSource);

    assertTimeoutOccurred(() -> store.putIfAbsent(1L, "one", b -> {}));
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
    @SuppressWarnings("unchecked")
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    @SuppressWarnings("unchecked")
    EternalChainResolver<Long, String> chainResolver = mock(EternalChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.getAndAppend(anyLong(), any())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, chainResolver, serverStoreProxy, testTimeSource);
    store.remove(1L, "one");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConditionalRemoveTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    when(proxy.getAndAppend(anyLong(), isNull())).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, null, proxy, timeSource);

    assertTimeoutOccurred(() -> store.remove(1L, "one"));
  }

  @Test
  public void testReplace() throws Exception {
    assertThat(store.replace(1L, "one"), nullValue());
    validateStats(store, EnumSet.of(StoreOperationOutcomes.ReplaceOutcome.MISS));
    store.put(1L, "one");
    assertThat(store.replace(1L, "another one").get(), is("one"));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.ReplaceOutcome.MISS, StoreOperationOutcomes.ReplaceOutcome.REPLACED));
  }

  @Test(expected = StoreAccessException.class)
  public void testReplaceThrowsOnlySAE() throws Exception {
    @SuppressWarnings("unchecked")
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    @SuppressWarnings("unchecked")
    EternalChainResolver<Long, String> chainResolver = mock(EternalChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.getAndAppend(anyLong(), any())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, chainResolver, serverStoreProxy, testTimeSource);
    store.replace(1L, "one");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReplaceTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    when(proxy.getAndAppend(anyLong(), isNull())).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, null, proxy, timeSource);

    assertTimeoutOccurred(() -> store.replace(1L, "one"));
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
    @SuppressWarnings("unchecked")
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    @SuppressWarnings("unchecked")
    EternalChainResolver<Long, String> chainResolver = mock(EternalChainResolver.class);
    ServerStoreProxy serverStoreProxy = mock(ServerStoreProxy.class);
    when(serverStoreProxy.getAndAppend(anyLong(), any())).thenThrow(new RuntimeException());
    TestTimeSource testTimeSource = mock(TestTimeSource.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, chainResolver, serverStoreProxy, testTimeSource);
    store.replace(1L, "one", "another one");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConditionalReplaceTimeout() throws Exception {
    ServerStoreProxy proxy = mock(ServerStoreProxy.class);
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);
    when(proxy.getAndAppend(anyLong(), isNull())).thenThrow(TimeoutException.class);
    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, null, proxy, timeSource);

    assertTimeoutOccurred(() -> store.replace(1L, "one", "another one"));
  }

  @Test
  public void testBulkComputePutAll() throws Exception {
    store.put(1L, "another one");
    Map<Long, String> map = new HashMap<>();
    map.put(1L, "one");
    map.put(2L, "two");
    Ehcache.PutAllFunction<Long, String> putAllFunction = new Ehcache.PutAllFunction<>(null, map, null);
    Map<Long, Store.ValueHolder<String>> valueHolderMap = store.bulkCompute(new HashSet<>(Arrays.asList(1L, 2L)), putAllFunction);

    assertThat(valueHolderMap.get(1L).get(), is(map.get(1L)));
    assertThat(store.get(1L).get(), is(map.get(1L)));
    assertThat(valueHolderMap.get(2L).get(), is(map.get(2L)));
    assertThat(store.get(2L).get(), is(map.get(2L)));
    assertThat(putAllFunction.getActualPutCount().get(), is(2));
    validateStats(store, EnumSet.of(StoreOperationOutcomes.PutOutcome.PUT));  //outcome of the initial store put
  }

  @Test
  public void testBulkComputeRemoveAll() throws Exception {
    store.put(1L, "one");
    store.put(2L, "two");
    store.put(3L, "three");
    Ehcache.RemoveAllFunction<Long, String> removeAllFunction = new Ehcache.RemoveAllFunction<>();
    Map<Long, Store.ValueHolder<String>> valueHolderMap = store.bulkCompute(new HashSet<>(Arrays.asList(1L, 2L, 4L)), removeAllFunction);

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
    @SuppressWarnings("unchecked")
    Function<Iterable<? extends Map.Entry<? extends Long, ? extends String>>, Iterable<? extends Map.Entry<? extends Long, ? extends String>>> remappingFunction
        = mock(Function.class);
    store.bulkCompute(new HashSet<>(Arrays.asList(1L, 2L)), remappingFunction);
  }

  @Test
  public void testBulkComputeIfAbsentGetAll() throws Exception {
    store.put(1L, "one");
    store.put(2L, "two");
    Ehcache.GetAllFunction<Long, String> getAllAllFunction = new Ehcache.GetAllFunction<>();
    Map<Long, Store.ValueHolder<String>> valueHolderMap = store.bulkComputeIfAbsent(new HashSet<>(Arrays.asList(1L, 2L)), getAllAllFunction);

    assertThat(valueHolderMap.get(1L).get(), is("one"));
    assertThat(store.get(1L).get(), is("one"));
    assertThat(valueHolderMap.get(2L).get(), is("two"));
    assertThat(store.get(2L).get(), is("two"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testBulkComputeIfAbsentThrowsForGenericFunction() throws Exception {
    @SuppressWarnings("unchecked")
    Function<Iterable<? extends Long>, Iterable<? extends Map.Entry<? extends Long, ? extends String>>> mappingFunction
        = mock(Function.class);
    store.bulkComputeIfAbsent(new HashSet<>(Arrays.asList(1L, 2L)), mappingFunction);
  }

  @Test
  public void testExpirationIsSentToHigherTiers() throws Exception {
    @SuppressWarnings("unchecked")
    Store.ValueHolder<String> valueHolder = mock(Store.ValueHolder.class, withSettings().defaultAnswer(RETURNS_MOCKS));
    when(valueHolder.get()).thenReturn("bar");
    when(valueHolder.expirationTime()).thenReturn(1000L);

    @SuppressWarnings("unchecked")
    EternalChainResolver<Long, String> resolver = mock(EternalChainResolver.class);
    when(resolver.resolve(any(ServerStoreProxy.ChainEntry.class), anyLong(), anyLong())).thenReturn(valueHolder);

    ServerStoreProxy proxy = mock(ServerStoreProxy.class, withSettings().defaultAnswer(RETURNS_MOCKS));

    @SuppressWarnings("unchecked")
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);

    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, resolver, proxy, timeSource);

    Store.ValueHolder<?> vh = store.get(1L);

    long expirationTime = vh.expirationTime();
    assertThat(expirationTime, is(1000L));
  }

  @Test
  public void testNoExpireIsSentToHigherTiers() throws Exception {
    @SuppressWarnings("unchecked")
    Store.ValueHolder<String> valueHolder = mock(Store.ValueHolder.class, withSettings().defaultAnswer(RETURNS_MOCKS));
    when(valueHolder.get()).thenReturn("bar");
    when(valueHolder.expirationTime()).thenReturn(NO_EXPIRE);

    @SuppressWarnings("unchecked")
    EternalChainResolver<Long, String> resolver = mock(EternalChainResolver.class);
    when(resolver.resolve(any(ServerStoreProxy.ChainEntry.class), anyLong(), anyLong())).thenReturn(valueHolder);

    ServerStoreProxy proxy = mock(ServerStoreProxy.class, withSettings().defaultAnswer(RETURNS_MOCKS));

    @SuppressWarnings("unchecked")
    OperationsCodec<Long, String> codec = mock(OperationsCodec.class);
    TimeSource timeSource = mock(TimeSource.class);

    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, resolver, proxy, timeSource);

    Store.ValueHolder<?> vh = store.get(1L);

    long expirationTime = vh.expirationTime();
    assertThat(expirationTime, is(NO_EXPIRE));
  }

  @Test
  public void testEmptyChainIteratorIsEmpty() throws StoreAccessException {

    Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator = store.iterator();

    assertThat(iterator.hasNext(), is(false));
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testSingleChainSingleValue() throws StoreAccessException {
    store.put(1L, "foo");

    Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator = store.iterator();

    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), isEntry(1L, "foo"));
    assertThat(iterator.hasNext(), is(false));
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testSingleChainMultipleValues() throws StoreAccessException {
    assertThat(Long.hashCode(1L), is(Long.hashCode(~1L)));

    store.put(1L, "foo");
    store.put(~1L, "bar");

    Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator = store.iterator();

    Matcher<Cache.Entry<Long, Store.ValueHolder<String>>> entryOne = isEntry(1L, "foo");
    Matcher<Cache.Entry<Long, Store.ValueHolder<String>>> entryTwo = isEntry(~1L, "bar");

    assertThat(iterator.hasNext(), is(true));

    Cache.Entry<Long, Store.ValueHolder<String>> next = iterator.next();
    assertThat(next, either(entryOne).or(entryTwo));

    if (entryOne.matches(next)) {
      assertThat(iterator.hasNext(), is(true));
      assertThat(iterator.next(), is(entryTwo));
      assertThat(iterator.hasNext(), is(false));
    } else {
      assertThat(iterator.hasNext(), is(true));
      assertThat(iterator.next(), is(entryOne));
      assertThat(iterator.hasNext(), is(false));
    }

    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testSingleChainRequiresResolution() throws StoreAccessException {

    store.put(~1L, "bar");
    store.put(1L, "foo");
    store.remove(~1L);

    Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator = store.iterator();

    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), isEntry(1L, "foo"));
    assertThat(iterator.hasNext(), is(false));
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testMultipleChains() throws StoreAccessException {

    store.put(1L, "foo");
    store.put(2L, "bar");

    Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator = store.iterator();

    Matcher<Cache.Entry<Long, Store.ValueHolder<String>>> entryOne = isEntry(1L, "foo");
    Matcher<Cache.Entry<Long, Store.ValueHolder<String>>> entryTwo = isEntry(2L, "bar");

    assertThat(iterator.hasNext(), is(true));

    Cache.Entry<Long, Store.ValueHolder<String>> next = iterator.next();
    assertThat(next, either(entryOne).or(entryTwo));

    if (entryOne.matches(next)) {
      assertThat(iterator.hasNext(), is(true));
      assertThat(iterator.next(), is(entryTwo));
      assertThat(iterator.hasNext(), is(false));
    } else {
      assertThat(iterator.hasNext(), is(true));
      assertThat(iterator.next(), is(entryOne));
      assertThat(iterator.hasNext(), is(false));
    }

    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  private <K, V> Matcher<Cache.Entry<K, Store.ValueHolder<V>>> isEntry(K key, V value) {
    return new TypeSafeMatcher<Cache.Entry<K, Store.ValueHolder<V>>>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(" the cache entry { ").appendValue(key).appendText(": ").appendValue(value).appendText(" }");
      }

      @Override
      protected boolean matchesSafely(Cache.Entry<K, Store.ValueHolder<V>> item) {
        return Objects.equal(key, item.getKey()) && Objects.equal(value, item.getValue().get());
      }
    };
  }
}
