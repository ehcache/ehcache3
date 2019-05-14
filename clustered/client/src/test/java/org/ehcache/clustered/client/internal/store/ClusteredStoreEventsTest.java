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

import org.ehcache.clustered.ChainUtils;
import org.ehcache.clustered.client.TestTimeSource;
import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityFactory;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.ServerCallback;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.client.internal.store.operations.ExpiryChainResolver;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.operations.ConditionalRemoveOperation;
import org.ehcache.clustered.common.internal.store.operations.ConditionalReplaceOperation;
import org.ehcache.clustered.common.internal.store.operations.Operation;
import org.ehcache.clustered.common.internal.store.operations.PutIfAbsentOperation;
import org.ehcache.clustered.common.internal.store.operations.PutOperation;
import org.ehcache.clustered.common.internal.store.operations.RemoveOperation;
import org.ehcache.clustered.common.internal.store.operations.ReplaceOperation;
import org.ehcache.clustered.common.internal.store.operations.TimestampOperation;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.events.StoreEventSink;
import org.ehcache.core.spi.store.Store;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.serialization.Serializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.terracotta.connection.Connection;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ClusteredStoreEventsTest {

  private static final String CACHE_IDENTIFIER = "testCache";
  private static final URI CLUSTER_URI = URI.create("terracotta://localhost");

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
  private StoreEventSink<Long, String> storeEventSink;
  private ServerCallback serverCallback;
  private OperationsCodec<Long, String> codec;
  private TestTimeSource testTimeSource;

  @SuppressWarnings("unchecked")
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

    testTimeSource = new TestTimeSource();

    codec = new OperationsCodec<>(new LongSerializer(), new StringSerializer());
    ChainResolver<Long, String> resolver = new ExpiryChainResolver<>(codec, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1000)));

    StoreEventDispatcher<Long, String> storeEventDispatcher = mock(StoreEventDispatcher.class);
    storeEventSink = mock(StoreEventSink.class);
    when(storeEventDispatcher.eventSink()).thenReturn(storeEventSink);

    ClusteredStore<Long, String> store = new ClusteredStore<>(config, codec, resolver, serverStoreProxy, testTimeSource, storeEventDispatcher);
    serverCallback = new ClusteredStore.Provider().getServerCallback(store);
  }

  @After
  public void tearDown() throws Exception {
    UnitTestConnectionService.remove("terracotta://localhost/my-application");
  }

  private ByteBuffer op(Operation<Long, String> operation) {
    return codec.encode(operation);
  }


  @Test
  public void testOnAppend_PutAfterNothingFiresCreatedEvent() {
    Chain beforeAppend = ChainUtils.chainOf();
    serverCallback.onAppend(beforeAppend, op(new PutOperation<>(1L, "one", testTimeSource.getTimeMillis())));

    verify(storeEventSink).created(eq(1L), eq("one"));
    verifyNoMoreInteractions(storeEventSink);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnAppend_PutAfterPutFiresUpdatedEvent() {
    Chain beforeAppend = ChainUtils.chainOf(op(new PutOperation<>(1L, "one", testTimeSource.getTimeMillis())));
    serverCallback.onAppend(beforeAppend, op(new PutOperation<>(1L, "one-bis", testTimeSource.getTimeMillis())));

    ArgumentCaptor<Supplier<String>> supplierArgumentCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(storeEventSink).updated(eq(1L), supplierArgumentCaptor.capture(), eq("one-bis"));
    verifyNoMoreInteractions(storeEventSink);
    assertThat(supplierArgumentCaptor.getValue().get(), is("one"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnAppend_RemoveAfterPutFiresRemovedEvent() {
    Chain beforeAppend = ChainUtils.chainOf(op(new PutOperation<>(1L, "one", testTimeSource.getTimeMillis())));
    serverCallback.onAppend(beforeAppend, op(new RemoveOperation<>(1L, testTimeSource.getTimeMillis())));

    ArgumentCaptor<Supplier<String>> supplierArgumentCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(storeEventSink).removed(eq(1L), supplierArgumentCaptor.capture());
    verifyNoMoreInteractions(storeEventSink);
    assertThat(supplierArgumentCaptor.getValue().get(), is("one"));
  }

  @Test
  public void testOnAppend_RemoveAfterNothingFiresNoEvent() {
    Chain beforeAppend = ChainUtils.chainOf();
    serverCallback.onAppend(beforeAppend, op(new RemoveOperation<>(1L, testTimeSource.getTimeMillis())));

    verifyNoMoreInteractions(storeEventSink);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnAppend_ReplaceAfterPutFiresUpdatedEvent() {
    Chain beforeAppend = ChainUtils.chainOf(op(new PutOperation<>(1L, "one", testTimeSource.getTimeMillis())));
    serverCallback.onAppend(beforeAppend, op(new ReplaceOperation<>(1L, "one-bis", testTimeSource.getTimeMillis())));

    ArgumentCaptor<Supplier<String>> supplierArgumentCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(storeEventSink).updated(eq(1L), supplierArgumentCaptor.capture(), eq("one-bis"));
    verifyNoMoreInteractions(storeEventSink);
    assertThat(supplierArgumentCaptor.getValue().get(), is("one"));
  }

  @Test
  public void testOnAppend_ReplaceAfterNothingFiresNoEvent() {
    Chain beforeAppend = ChainUtils.chainOf();
    serverCallback.onAppend(beforeAppend, op(new ReplaceOperation<>(1L, "one", testTimeSource.getTimeMillis())));

    verifyNoMoreInteractions(storeEventSink);
  }

  @Test
  public void testOnAppend_PutIfAbsentAfterNothingFiresCreatedEvent() {
    Chain beforeAppend = ChainUtils.chainOf();
    serverCallback.onAppend(beforeAppend, op(new PutIfAbsentOperation<>(1L, "one", testTimeSource.getTimeMillis())));

    verify(storeEventSink).created(eq(1L), eq("one"));
    verifyNoMoreInteractions(storeEventSink);
  }

  @Test
  public void testOnAppend_PutIfAbsentAfterPutFiresNoEvent() {
    Chain beforeAppend = ChainUtils.chainOf(op(new PutOperation<>(1L, "one", testTimeSource.getTimeMillis())));
    serverCallback.onAppend(beforeAppend, op(new PutIfAbsentOperation<>(1L, "one-bis", testTimeSource.getTimeMillis())));

    verifyNoMoreInteractions(storeEventSink);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnAppend_SuccessfulReplaceConditionalAfterPutFiresUpdatedEvent() {
    Chain beforeAppend = ChainUtils.chainOf(op(new PutOperation<>(1L, "one", testTimeSource.getTimeMillis())));
    serverCallback.onAppend(beforeAppend, op(new ConditionalReplaceOperation<>(1L, "one", "one-bis", testTimeSource.getTimeMillis())));

    ArgumentCaptor<Supplier<String>> supplierArgumentCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(storeEventSink).updated(eq(1L), supplierArgumentCaptor.capture(), eq("one-bis"));
    verifyNoMoreInteractions(storeEventSink);
    assertThat(supplierArgumentCaptor.getValue().get(), is("one"));
  }

  @Test
  public void testOnAppend_FailingReplaceConditionalAfterPutFiresNoEvent() {
    Chain beforeAppend = ChainUtils.chainOf(op(new PutOperation<>(1L, "one", testTimeSource.getTimeMillis())));
    serverCallback.onAppend(beforeAppend, op(new ConditionalReplaceOperation<>(1L, "un", "one-bis", testTimeSource.getTimeMillis())));

    verifyNoMoreInteractions(storeEventSink);
  }

  @Test
  public void testOnAppend_ReplaceConditionalAfterNothingFiresNoEvent() {
    Chain beforeAppend = ChainUtils.chainOf();
    serverCallback.onAppend(beforeAppend, op(new ConditionalReplaceOperation<>(1L, "one", "one-bis", testTimeSource.getTimeMillis())));

    verifyNoMoreInteractions(storeEventSink);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnAppend_SuccessfulRemoveConditionalAfterPutFiresUpdatedEvent() {
    Chain beforeAppend = ChainUtils.chainOf(op(new PutOperation<>(1L, "one", testTimeSource.getTimeMillis())));
    serverCallback.onAppend(beforeAppend, op(new ConditionalRemoveOperation<>(1L, "one", testTimeSource.getTimeMillis())));

    ArgumentCaptor<Supplier<String>> supplierArgumentCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(storeEventSink).removed(eq(1L), supplierArgumentCaptor.capture());
    verifyNoMoreInteractions(storeEventSink);
    assertThat(supplierArgumentCaptor.getValue().get(), is("one"));
  }

  @Test
  public void testOnAppend_FailingRemoveConditionalAfterPutFiresNoEvent() {
    Chain beforeAppend = ChainUtils.chainOf(op(new PutOperation<>(1L, "one", testTimeSource.getTimeMillis())));
    serverCallback.onAppend(beforeAppend, op(new ConditionalRemoveOperation<>(1L, "un", testTimeSource.getTimeMillis())));

    verifyNoMoreInteractions(storeEventSink);
  }

  @Test
  public void testOnAppend_RemoveConditionalAfterNothingFiresNoEvent() {
    Chain beforeAppend = ChainUtils.chainOf();
    serverCallback.onAppend(beforeAppend, op(new ConditionalRemoveOperation<>(1L, "one", testTimeSource.getTimeMillis())));

    verifyNoMoreInteractions(storeEventSink);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnAppend_timestampAfterExpiryFiresExpiredEvent() {
    Chain beforeAppend = ChainUtils.chainOf(op(new PutOperation<>(1L, "wrong-one", testTimeSource.getTimeMillis())), op(new PutOperation<>(1L, "one", testTimeSource.getTimeMillis())));
    testTimeSource.advanceTime(1100L);
    serverCallback.onAppend(beforeAppend, op(new TimestampOperation<>(1L, testTimeSource.getTimeMillis())));

    ArgumentCaptor<Supplier<String>> supplierArgumentCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(storeEventSink).expired(eq(1L), supplierArgumentCaptor.capture());
    verifyNoMoreInteractions(storeEventSink);
    assertThat(supplierArgumentCaptor.getValue().get(), is("one"));
  }

  @Test
  public void testOnAppend_timestampAfterNothingFiresNoEvent() {
    Chain beforeAppend = ChainUtils.chainOf();
    serverCallback.onAppend(beforeAppend, op(new TimestampOperation<>(1L, testTimeSource.getTimeMillis())));

    verifyNoMoreInteractions(storeEventSink);
  }

  @Test
  public void testOnAppend_timestampAfterNoExpiryFiresNoEvent() {
    Chain beforeAppend = ChainUtils.chainOf(op(new PutOperation<>(1L, "one", testTimeSource.getTimeMillis())));
    testTimeSource.advanceTime(100L);
    serverCallback.onAppend(beforeAppend, op(new TimestampOperation<>(1L, testTimeSource.getTimeMillis())));

    verifyNoMoreInteractions(storeEventSink);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnInvalidateHash_chainFiresEvictedEvents() {
    Chain evictedChain = ChainUtils.chainOf(op(new PutOperation<>(1L, "one", testTimeSource.getTimeMillis())), op(new PutOperation<>(2L, "two", testTimeSource.getTimeMillis())));
    serverCallback.onInvalidateHash(1L, evictedChain);

    ArgumentCaptor<Supplier<String>> supplierArgumentCaptor1 = ArgumentCaptor.forClass(Supplier.class);
    ArgumentCaptor<Supplier<String>> supplierArgumentCaptor2 = ArgumentCaptor.forClass(Supplier.class);
    verify(storeEventSink).evicted(eq(1L), supplierArgumentCaptor1.capture());
    verify(storeEventSink).evicted(eq(2L), supplierArgumentCaptor2.capture());
    verifyNoMoreInteractions(storeEventSink);
    assertThat(supplierArgumentCaptor1.getValue().get(), is("one"));
    assertThat(supplierArgumentCaptor2.getValue().get(), is("two"));
  }

  @Test
  public void testOnInvalidateHash_noChainFiresNoEvent() {
    serverCallback.onInvalidateHash(1L, null);

    verifyNoMoreInteractions(storeEventSink);
  }
}
