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
package org.ehcache.clustered.client.internal.loaderwriter;

import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.lock.LockingServerStoreProxyImpl;
import org.ehcache.clustered.client.internal.store.operations.EternalChainResolver;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.operations.PutOperation;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.loaderWriter.TestCacheLoaderWriter;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.ChainUtils.chainOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ClusteredLoaderWriterStoreTest {

  @SuppressWarnings("unchecked")
  private Store.Configuration<Long, String> configuration = mock(Store.Configuration.class);
  private OperationsCodec<Long, String> codec = new OperationsCodec<>(new LongSerializer(), new StringSerializer());
  private EternalChainResolver<Long, String> resolver = new EternalChainResolver<>(codec);
  private TimeSource timeSource = mock(TimeSource.class);

  @Test
  public void testGetValueAbsentInSOR() throws Exception {
    ServerStoreProxy storeProxy = mock(LockingServerStoreProxyImpl.class);
    CacheLoaderWriter<Long, String> loaderWriter = new TestCacheLoaderWriter();
    when(storeProxy.get(eq(1L))).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    assertThat(store.get(1L), is(nullValue()));
  }

  @Test
  public void testGetValuePresentInSOR() throws Exception {
    ServerStoreProxy storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    loaderWriter.storeMap.put(1L, "one");
    when(storeProxy.get(eq(1L))).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    assertThat(store.get(1L).get(), equalTo("one"));
  }

  @Test
  public void testGetValuePresentInCache() throws Exception {
    ServerStoreProxy storeProxy = mock(LockingServerStoreProxyImpl.class);
    @SuppressWarnings("unchecked")
    CacheLoaderWriter<Long, String> loaderWriter = mock(CacheLoaderWriter.class);
    PutOperation<Long, String> operation = new PutOperation<>(1L, "one", System.currentTimeMillis());
    ServerStoreProxy.ChainEntry toReturn = entryOf(codec.encode(operation));
    when(storeProxy.get(anyLong())).thenReturn(toReturn);
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    assertThat(store.get(1L).get(), equalTo("one"));
    verify(loaderWriter, times(0)).load(anyLong());
    verifyZeroInteractions(loaderWriter);
  }

  @Test
  public void testPut() throws Exception {
    ServerStoreProxy storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    assertThat(loaderWriter.storeMap.containsKey(1L), is(false));
    assertThat(store.put(1L, "one"), is(Store.PutStatus.PUT));
    assertThat(loaderWriter.storeMap.containsKey(1L), is(true));
  }

  @Test
  public void testRemoveValueAbsentInCachePresentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    when(storeProxy.lock(anyLong())).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    loaderWriter.storeMap.put(1L, "one");
    assertThat(store.remove(1L), is(false));
    assertThat(loaderWriter.storeMap.containsKey(1L), is(false));
  }

  @Test
  public void testRemoveValuePresentInCachePresentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    PutOperation<Long, String> operation = new PutOperation<>(1L, "one", System.currentTimeMillis());
    ServerStoreProxy.ChainEntry toReturn = entryOf(codec.encode(operation));
    when(storeProxy.lock(anyLong())).thenReturn(toReturn);
    when(storeProxy.get(anyLong())).thenReturn(toReturn);
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    loaderWriter.storeMap.put(1L, "one");
    assertThat(store.get(1L).get(), equalTo("one"));
    assertThat(store.remove(1L), is(true));
    assertThat(loaderWriter.storeMap.containsKey(1L), is(false));
  }

  @Test
  public void testRemoveValueAbsentInCacheAbsentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    @SuppressWarnings("unchecked")
    CacheLoaderWriter<Long, String> loaderWriter = mock(CacheLoaderWriter.class);
    when(storeProxy.lock(anyLong())).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    assertThat(store.remove(1L), is(false));
    verify(loaderWriter, times(1)).delete(anyLong());
  }

  @Test
  public void testPufIfAbsentValueAbsentInCacheAbsentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    when(storeProxy.lock(anyLong())).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    assertThat(loaderWriter.storeMap.isEmpty(), is(true));
    assertThat(store.putIfAbsent(1L, "one", null), is(nullValue()));
    assertThat(loaderWriter.storeMap.get(1L), equalTo("one"));
  }

  @Test
  public void testPufIfAbsentValueAbsentInCachePresentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    when(storeProxy.lock(anyLong())).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    loaderWriter.storeMap.put(1L, "one");
    assertThat(store.putIfAbsent(1L, "Again", null).get(), equalTo("one"));
    verify(storeProxy, times(0)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    assertThat(loaderWriter.storeMap.get(1L), equalTo("one"));
  }

  @Test
  public void testPufIfAbsentValuePresentInCachePresentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    PutOperation<Long, String> operation = new PutOperation<>(1L, "one", System.currentTimeMillis());
    ServerStoreProxy.ChainEntry toReturn = entryOf(codec.encode(operation));
    when(storeProxy.lock(anyLong())).thenReturn(toReturn);
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    loaderWriter.storeMap.put(1L, "one");
    assertThat(store.putIfAbsent(1L, "Again", null).get(), equalTo("one"));
    verify(storeProxy, times(0)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    assertThat(loaderWriter.storeMap.get(1L), equalTo("one"));
  }

  @Test
  public void testReplaceValueAbsentInCacheAbsentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    when(storeProxy.lock(anyLong())).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    assertThat(loaderWriter.storeMap.isEmpty(), is(true));
    assertThat(store.replace(1L, "one"), is(nullValue()));
    assertThat(loaderWriter.storeMap.isEmpty(), is(true));
    verify(storeProxy, times(0)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
  }

  @Test
  public void testReplaceValueAbsentInCachePresentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    when(storeProxy.lock(anyLong())).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    loaderWriter.storeMap.put(1L, "one");
    assertThat(store.replace(1L, "Again").get(), equalTo("one"));
    verify(storeProxy, times(1)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    assertThat(loaderWriter.storeMap.get(1L), equalTo("Again"));
  }

  @Test
  public void testReplaceValuePresentInCachePresentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    PutOperation<Long, String> operation = new PutOperation<>(1L, "one", System.currentTimeMillis());
    ServerStoreProxy.ChainEntry toReturn = entryOf(codec.encode(operation));
    when(storeProxy.lock(anyLong())).thenReturn(toReturn);
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    loaderWriter.storeMap.put(1L, "one");
    assertThat(store.replace(1L, "Again").get(), equalTo("one"));
    verify(storeProxy, times(1)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    assertThat(loaderWriter.storeMap.get(1L), equalTo("Again"));
  }

  @Test
  public void testRemove2ArgsValueAbsentInCacheAbsentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    @SuppressWarnings("unchecked")
    CacheLoaderWriter<Long, String> loaderWriter = mock(CacheLoaderWriter.class);
    when(storeProxy.lock(anyLong())).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    assertThat(store.remove(1L, "one"), is(Store.RemoveStatus.KEY_MISSING));
    verify(storeProxy, times(0)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
  }

  @Test
  public void testRemove2ArgsValueAbsentInCachePresentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    when(storeProxy.lock(anyLong())).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    loaderWriter.storeMap.put(1L, "one");
    assertThat(store.remove(1L, "one"), is(Store.RemoveStatus.REMOVED));
    verify(storeProxy, times(1)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    assertThat(loaderWriter.storeMap.isEmpty(), is(true));
  }

  @Test
  public void testRemove2ArgsValuePresentInCachePresentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    @SuppressWarnings("unchecked")
    CacheLoaderWriter<Long, String> loaderWriter = mock(CacheLoaderWriter.class);
    PutOperation<Long, String> operation = new PutOperation<>(1L, "one", System.currentTimeMillis());
    ServerStoreProxy.ChainEntry toReturn = entryOf(codec.encode(operation));
    when(storeProxy.lock(anyLong())).thenReturn(toReturn);
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    assertThat(store.remove(1L, "one"), is(Store.RemoveStatus.REMOVED));
    verify(storeProxy, times(1)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    verify(loaderWriter, times(0)).load(anyLong());
    verify(loaderWriter, times(1)).delete(anyLong());
  }

  @Test
  public void testRemove2ArgsValueAbsentInCacheDiffValuePresentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    when(storeProxy.lock(anyLong())).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    loaderWriter.storeMap.put(1L, "one");
    assertThat(store.remove(1L, "Again"), is(Store.RemoveStatus.KEY_PRESENT));
    verify(storeProxy, times(0)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    assertThat(loaderWriter.storeMap.get(1L), equalTo("one"));
  }

  @Test
  public void testReplace2ArgsValueAbsentInCacheAbsentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    @SuppressWarnings("unchecked")
    CacheLoaderWriter<Long, String> loaderWriter = mock(CacheLoaderWriter.class);
    when(storeProxy.lock(anyLong())).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    assertThat(store.replace(1L, "one", "Again"), is(Store.ReplaceStatus.MISS_NOT_PRESENT));
    verify(storeProxy, times(0)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    verify(loaderWriter, times(1)).load(anyLong());
    verify(loaderWriter, times(0)).write(anyLong(), anyString());
  }

  @Test
  public void testReplace2ArgsValueAbsentInCachePresentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    when(storeProxy.lock(anyLong())).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    loaderWriter.storeMap.put(1L, "one");
    assertThat(store.replace(1L, "one", "Again"), is(Store.ReplaceStatus.HIT));
    verify(storeProxy, times(1)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    assertThat(loaderWriter.storeMap.get(1L), equalTo("Again"));
  }

  @Test
  public void testReplace2ArgsValuePresentInCachePresentInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    @SuppressWarnings("unchecked")
    CacheLoaderWriter<Long, String> loaderWriter = mock(CacheLoaderWriter.class);
    PutOperation<Long, String> operation = new PutOperation<>(1L, "one", System.currentTimeMillis());
    ServerStoreProxy.ChainEntry toReturn = entryOf(codec.encode(operation));
    when(storeProxy.lock(anyLong())).thenReturn(toReturn);
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    assertThat(store.replace(1L, "one", "Again"), is(Store.ReplaceStatus.HIT));
    verify(storeProxy, times(1)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    verify(loaderWriter, times(0)).load(anyLong());
    verify(loaderWriter, times(1)).write(anyLong(), anyString());
  }

  @Test
  public void testReplace2ArgsValueAbsentInCacheDiffValueInSOR() throws Exception {
    LockingServerStoreProxyImpl storeProxy = mock(LockingServerStoreProxyImpl.class);
    TestCacheLoaderWriter loaderWriter = new TestCacheLoaderWriter();
    when(storeProxy.lock(anyLong())).thenReturn(entryOf());
    ClusteredLoaderWriterStore<Long, String> store = new ClusteredLoaderWriterStore<>(configuration, codec, resolver, storeProxy,
            timeSource, loaderWriter);
    loaderWriter.storeMap.put(1L, "one");
    assertThat(store.replace(1L, "Again", "one"), is(Store.ReplaceStatus.MISS_PRESENT));
    verify(storeProxy, times(0)).append(anyLong(), ArgumentMatchers.any(ByteBuffer.class));
    assertThat(loaderWriter.storeMap.get(1L), equalTo("one"));
  }

  private static ServerStoreProxy.ChainEntry entryOf(ByteBuffer ... elements) {
    Chain chain = chainOf(elements);
    return new ServerStoreProxy.ChainEntry() {
      @Override
      public void append(ByteBuffer payLoad) throws TimeoutException {
        throw new AssertionError();
      }

      @Override
      public void replaceAtHead(Chain equivalent) {
        throw new AssertionError();
      }

      @Override
      public boolean isEmpty() {
        return chain.isEmpty();
      }

      @Override
      public int length() {
        return chain.length();
      }

      @Override
      public Iterator<Element> iterator() {
        return chain.iterator();
      }
    };
  }
}
