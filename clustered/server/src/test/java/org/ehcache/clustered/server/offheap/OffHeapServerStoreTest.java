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
package org.ehcache.clustered.server.offheap;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.store.ChainBuilder;
import org.ehcache.clustered.server.store.ElementBuilder;
import org.ehcache.clustered.common.internal.store.ServerStore;
import org.ehcache.clustered.server.store.ServerStoreTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.ehcache.clustered.ChainUtils.chainOf;
import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.terracotta.offheapstore.util.MemoryUnit.GIGABYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

public class OffHeapServerStoreTest extends ServerStoreTest {

  private static final KeySegmentMapper DEFAULT_MAPPER = new KeySegmentMapper(16);

  @SuppressWarnings("unchecked")
  private OffHeapChainMap<Object> getOffHeapChainMapMock() {
    return mock(OffHeapChainMap.class);
  }

  @SuppressWarnings("unchecked")
  private OffHeapChainMap<Long> getOffHeapChainMapLongMock() {
    return mock(OffHeapChainMap.class);
  }

  @SuppressWarnings("unchecked")
  private ChainStorageEngine<Long> getChainStorageEngineLongMock() {
    return mock(ChainStorageEngine.class);
  }

  @Override
  public ServerStore newStore() {
    return new OffHeapServerStore(new UnlimitedPageSource(new OffHeapBufferSource()), DEFAULT_MAPPER, false);
  }

  @Override
  public ChainBuilder newChainBuilder() {
    return elements -> {
      ByteBuffer[] buffers = new ByteBuffer[elements.length];
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = elements[i].getPayload();
      }
      return chainOf(buffers);
    };
  }

  @Override
  public ElementBuilder newElementBuilder() {
    return payLoad -> () -> payLoad.asReadOnlyBuffer();
  }

  @Test
  public void testGetMaxSize() {
    assertThat(OffHeapServerStore.getMaxSize(MEGABYTES.toBytes(2)), is(64L));
    assertThat(OffHeapServerStore.getMaxSize(MEGABYTES.toBytes(4)), is(128L));
    assertThat(OffHeapServerStore.getMaxSize(MEGABYTES.toBytes(16)), is(512L));
    assertThat(OffHeapServerStore.getMaxSize(MEGABYTES.toBytes(64)), is(2048L));
    assertThat(OffHeapServerStore.getMaxSize(MEGABYTES.toBytes(128)), is(4096L));
    assertThat(OffHeapServerStore.getMaxSize(MEGABYTES.toBytes(256)), is(8192L));
    assertThat(OffHeapServerStore.getMaxSize(MEGABYTES.toBytes(512)), is(8192L));

    assertThat(OffHeapServerStore.getMaxSize(GIGABYTES.toBytes(2)), is(8192L));
  }

  @Test
  public void put_worked_the_first_time_test() throws Exception {
    OffHeapChainMap<Long> offheapChainMap = getOffHeapChainMapLongMock();
    ChainStorageEngine<Long> storageEngine = getChainStorageEngineLongMock();
    when(offheapChainMap.getStorageEngine()).thenReturn(storageEngine);

    doNothing()
      .when(offheapChainMap).put(anyLong(), any(Chain.class));

    OffHeapServerStore offHeapServerStore = new OffHeapServerStore(singletonList(offheapChainMap), mock(KeySegmentMapper.class));
    offHeapServerStore.put(43L, mock(Chain.class));
  }


  @Test(expected = OversizeMappingException.class)
  public void put_should_throw_when_underlying_put_always_throw_test() throws Exception {
    OffHeapChainMap<Long> offheapChainMap = getOffHeapChainMapLongMock();
    ChainStorageEngine<Long> storageEngine = getChainStorageEngineLongMock();
    when(offheapChainMap.getStorageEngine()).thenReturn(storageEngine);
    when(offheapChainMap.writeLock()).thenReturn(new ReentrantLock());

    doThrow(new OversizeMappingException())
      .when(offheapChainMap).put(anyLong(), any(Chain.class));

    OffHeapServerStore offHeapServerStore = new OffHeapServerStore(singletonList(offheapChainMap), mock(KeySegmentMapper.class));
    offHeapServerStore.put(43L, mock(Chain.class));
  }

  @Test
  public void put_should_return_when_underlying_put_does_not_throw_test() throws Exception {
    OffHeapChainMap<Long> offheapChainMap = getOffHeapChainMapLongMock();
    ChainStorageEngine<Long> storageEngine = getChainStorageEngineLongMock();
    when(offheapChainMap.getStorageEngine()).thenReturn(storageEngine);
    when(offheapChainMap.writeLock()).thenReturn(new ReentrantLock());

    // throw once, then ok
    doThrow(new OversizeMappingException())
      .doNothing()
      .when(offheapChainMap).put(anyLong(), any(Chain.class));

    OffHeapServerStore offHeapServerStore = new OffHeapServerStore(singletonList(offheapChainMap), mock(KeySegmentMapper.class));
    offHeapServerStore.put(43L, mock(Chain.class));
  }

  @Test
  public void put_should_return_when_underlying_put_does_not_throw_with_keymapper_test() throws Exception {
    long theKey = 43L;
    ChainStorageEngine<Long> storageEngine = getChainStorageEngineLongMock();
    OffHeapChainMap<Long> offheapChainMap = getOffHeapChainMapLongMock();
    OffHeapChainMap<Long> otherOffheapChainMap = getOffHeapChainMapLongMock();
    when(offheapChainMap.shrink()).thenReturn(true);
    when(offheapChainMap.getStorageEngine()).thenReturn(storageEngine);
    when(offheapChainMap.writeLock()).thenReturn(new ReentrantLock());
    when(otherOffheapChainMap.writeLock()).thenReturn(new ReentrantLock());

    // throw twice, then OK
    doThrow(new OversizeMappingException())
      .doThrow(new OversizeMappingException())
      .doNothing()
      .when(otherOffheapChainMap).put(anyLong(), any(Chain.class));

    KeySegmentMapper keySegmentMapper = mock(KeySegmentMapper.class);
    when(keySegmentMapper.getSegmentForKey(theKey)).thenReturn(1);
    OffHeapServerStore offHeapServerStore = new OffHeapServerStore(asList(offheapChainMap, otherOffheapChainMap), keySegmentMapper);
    offHeapServerStore.put(theKey, mock(Chain.class));

    //getSegmentForKey was called 4 times : segmentFor, handleOversizeMappingException, segmentFor, segmentFor
    verify(keySegmentMapper, times(4)).getSegmentForKey(theKey);
  }


  @Test
  public void test_append_doesNotConsumeBuffer_evenWhenOversizeMappingException() throws Exception {
    OffHeapServerStore store = (OffHeapServerStore) spy(newStore());
    final OffHeapChainMap<Object> offHeapChainMap = getOffHeapChainMapMock();
    doThrow(OversizeMappingException.class).when(offHeapChainMap).append(any(Object.class), any(ByteBuffer.class));

    when(store.segmentFor(anyLong())).then(new Answer<Object>() {
      int invocations = 0;
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (invocations++ < 10) {
          return offHeapChainMap;
        } else {
          return invocation.callRealMethod();
        }
      }
    });
    when(store.tryShrinkOthers(anyLong())).thenReturn(true);

    ByteBuffer payload = createPayload(1L);

    store.append(1L, payload);
    assertThat(payload.remaining(), is(8));
  }

  @Test
  public void test_getAndAppend_doesNotConsumeBuffer_evenWhenOversizeMappingException() throws Exception {
    OffHeapServerStore store = (OffHeapServerStore) spy(newStore());
    final OffHeapChainMap<Object> offHeapChainMap = getOffHeapChainMapMock();
    doThrow(OversizeMappingException.class).when(offHeapChainMap).getAndAppend(any(), any(ByteBuffer.class));

    when(store.segmentFor(anyLong())).then(new Answer<Object>() {
      int invocations = 0;
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (invocations++ < 10) {
          return offHeapChainMap;
        } else {
          return invocation.callRealMethod();
        }
      }
    });
    when(store.tryShrinkOthers(anyLong())).thenReturn(true);


    ByteBuffer payload = createPayload(1L);

    store.getAndAppend(1L, payload);
    assertThat(payload.remaining(), is(8));

    Chain expected = newChainBuilder().build(newElementBuilder().build(payload), newElementBuilder().build(payload));
    Chain update = newChainBuilder().build(newElementBuilder().build(payload));
    store.replaceAtHead(1L, expected, update);
    assertThat(payload.remaining(), is(8));
  }

  @Test
  public void test_replaceAtHead_doesNotConsumeBuffer_evenWhenOversizeMappingException() throws Exception {
    OffHeapServerStore store = (OffHeapServerStore) spy(newStore());
    final OffHeapChainMap<Object> offHeapChainMap = getOffHeapChainMapMock();
    doThrow(OversizeMappingException.class).when(offHeapChainMap).replaceAtHead(any(), any(Chain.class), any(Chain.class));

    when(store.segmentFor(anyLong())).then(new Answer<Object>() {
      int invocations = 0;
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (invocations++ < 10) {
          return offHeapChainMap;
        } else {
          return invocation.callRealMethod();
        }
      }
    });
    when(store.tryShrinkOthers(anyLong())).thenReturn(true);


    ByteBuffer payload = createPayload(1L);

    Chain expected = newChainBuilder().build(newElementBuilder().build(payload), newElementBuilder().build(payload));
    Chain update = newChainBuilder().build(newElementBuilder().build(payload));
    store.replaceAtHead(1L, expected, update);
    assertThat(payload.remaining(), is(8));
  }

  @Test
  public void testCrossSegmentShrinking() {
    long seed = System.nanoTime();
    Random random = new Random(seed);
    try {
      OffHeapServerStore store = new OffHeapServerStore(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MEGABYTES.toBytes(1L), MEGABYTES.toBytes(1)), DEFAULT_MAPPER, false);

      ByteBuffer smallValue = ByteBuffer.allocate(1024);
      for (int i = 0; i < 10000; i++) {
        try {
          store.getAndAppend(random.nextInt(500), smallValue.duplicate());
        } catch (OversizeMappingException e) {
          //ignore
        }
      }

      ByteBuffer largeValue = ByteBuffer.allocate(100 * 1024);
      for (int i = 0; i < 10000; i++) {
        try {
          store.getAndAppend(random.nextInt(500), largeValue.duplicate());
        } catch (OversizeMappingException e) {
          //ignore
        }
      }
    } catch (Throwable t) {
      throw (AssertionError) new AssertionError("Failed with seed " + seed).initCause(t);
    }
  }

  @Test
  public void testServerSideUsageStats() {

    long maxBytes = MEGABYTES.toBytes(1);
    OffHeapServerStore store = new OffHeapServerStore(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), maxBytes, MEGABYTES.toBytes(1)), new KeySegmentMapper(16), false);

    int oneKb = 1024;
    long smallLoopCount = 5;
    ByteBuffer smallValue = ByteBuffer.allocate(oneKb);
    for (long i = 0; i < smallLoopCount; i++) {
      store.getAndAppend(i, smallValue.duplicate());
    }

    Assert.assertThat(store.getAllocatedMemory(),lessThanOrEqualTo(maxBytes));
    Assert.assertThat(store.getAllocatedMemory(),greaterThanOrEqualTo(smallLoopCount * oneKb));
    Assert.assertThat(store.getAllocatedMemory(),greaterThanOrEqualTo(store.getOccupiedMemory()));

    //asserts above already guarantee that occupiedMemory <= maxBytes and that occupiedMemory <= allocatedMemory
    Assert.assertThat(store.getOccupiedMemory(),greaterThanOrEqualTo(smallLoopCount * oneKb));

    Assert.assertThat(store.getSize(), is(smallLoopCount));

    int multiplier = 100;
    long largeLoopCount = 5 + smallLoopCount;
    ByteBuffer largeValue = ByteBuffer.allocate(multiplier * oneKb);
    for (long i = smallLoopCount; i < largeLoopCount; i++) {
      store.getAndAppend(i, largeValue.duplicate());
    }

    Assert.assertThat(store.getAllocatedMemory(),lessThanOrEqualTo(maxBytes));
    Assert.assertThat(store.getAllocatedMemory(),greaterThanOrEqualTo( (smallLoopCount * oneKb) + ( (largeLoopCount - smallLoopCount) * oneKb * multiplier) ));
    Assert.assertThat(store.getAllocatedMemory(),greaterThanOrEqualTo(store.getOccupiedMemory()));

    //asserts above already guarantee that occupiedMemory <= maxBytes and that occupiedMemory <= allocatedMemory
    Assert.assertThat(store.getOccupiedMemory(),greaterThanOrEqualTo(smallLoopCount * oneKb));

    Assert.assertThat(store.getSize(), is(smallLoopCount + (largeLoopCount - smallLoopCount)));

  }

}
