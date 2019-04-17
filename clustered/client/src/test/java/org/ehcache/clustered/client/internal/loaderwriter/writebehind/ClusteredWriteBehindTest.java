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
package org.ehcache.clustered.client.internal.loaderwriter.writebehind;

import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.common.internal.util.ChainBuilder;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.client.internal.store.operations.ExpiryChainResolver;
import org.ehcache.clustered.common.internal.store.operations.ConditionalRemoveOperation;
import org.ehcache.clustered.common.internal.store.operations.Operation;
import org.ehcache.clustered.common.internal.store.operations.PutIfAbsentOperation;
import org.ehcache.clustered.common.internal.store.operations.PutOperation;
import org.ehcache.clustered.common.internal.store.operations.PutWithWriterOperation;
import org.ehcache.clustered.common.internal.store.operations.RemoveOperation;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.loaderWriter.writebehind.RecordingLoaderWriter;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusteredWriteBehindTest {

  private static final TimeSource TIME_SOURCE = SystemTimeSource.INSTANCE;

  @Test
  public void testPutWithWriter() throws Exception {
    List<EventInfo> eventInfoList = new ArrayList<>();
    eventInfoList.add(new EventInfo(1L,
                                    new PutWithWriterOperation<>(1L, "The one", TIME_SOURCE.getTimeMillis()),
                                    "The one",
                                    true));
    eventInfoList.add(new EventInfo(1L,
                                    new PutWithWriterOperation<>(1L, "The one one", TIME_SOURCE.getTimeMillis()),
                                    "The one one",
                                    true));
    eventInfoList.add(new EventInfo(2L,
                                    new PutWithWriterOperation<>(2L, "The two", TIME_SOURCE.getTimeMillis()),

                                    "The two",
                                    true));
    eventInfoList.add(new EventInfo(2L,
                                    new PutWithWriterOperation<>(2L, "The two two", TIME_SOURCE.getTimeMillis()),
                                    "The two two",
                                    true));

    HashMap<Long, String> result = new HashMap<>();
    result.put(1L, "The one one");
    result.put(2L, "The two two");
    verifyEvents(eventInfoList, result);
  }

  @Test
  public void testRemoves() throws Exception {
    List<EventInfo> eventInfoList = new ArrayList<>();
    eventInfoList.add(new EventInfo(1L,
                                    new PutWithWriterOperation<>(1L, "The one", TIME_SOURCE.getTimeMillis()),
                                    "The one",
                                    true));
    eventInfoList.add(new EventInfo(1L,
                                    new PutWithWriterOperation<>(1L, "The one one", TIME_SOURCE.getTimeMillis()),
                                    "The one one",
                                    true));
    eventInfoList.add(new EventInfo(1L, new RemoveOperation<>(1L, TIME_SOURCE.getTimeMillis()), null, true));

    verifyEvents(eventInfoList, Collections.emptyMap());
  }

  @Test
  public void testCAS() throws Exception {
    List<EventInfo> eventInfoList = new ArrayList<>();
    eventInfoList.add(new EventInfo(1L,
                                    new PutIfAbsentOperation<>(1L, "The one", TIME_SOURCE.getTimeMillis()),
                                    "The one",
                                    true));
    eventInfoList.add(new EventInfo(1L,
                                    new PutIfAbsentOperation<>(1L, "The one one", TIME_SOURCE.getTimeMillis()),
                                    "none",
                                    false));
    eventInfoList.add(new EventInfo(1L,
                                    new ConditionalRemoveOperation<>(1L, "The one", TIME_SOURCE.getTimeMillis()),
                                    null,
                                    true));

    verifyEvents(eventInfoList, Collections.emptyMap());
  }

  @Test
  public void testPuts() throws Exception {
    List<EventInfo> eventInfoList = new ArrayList<>();
    eventInfoList.add(new EventInfo(1L,
                                    new PutOperation<>(1L, "The one", TIME_SOURCE.getTimeMillis()),
                                    "The one",
                                    false));
    eventInfoList.add(new EventInfo(1L,
                                    new PutWithWriterOperation<>(1L, "The one one", TIME_SOURCE.getTimeMillis()),
                                    "The one one",
                                    true));
    eventInfoList.add(new EventInfo(2L, new PutWithWriterOperation<>(2L, "The two", TIME_SOURCE.getTimeMillis()),
                                    "The two",
                                    true));
    eventInfoList.add(new EventInfo(4L, new PutWithWriterOperation<>(4L, "The four", TIME_SOURCE.getTimeMillis()),
                                    "The four",
                                    true));

    HashMap<Long, String> result = new HashMap<>();
    result.put(1L, "The one one");
    result.put(2L, "The two");
    result.put(4L, "The four");
    verifyEvents(eventInfoList, result);
  }

  @SuppressWarnings("unchecked")
  private void verifyEvents(List<EventInfo> expected, Map<Long, String> expectedChainContents) throws TimeoutException {
    ClusteredWriteBehindStore<Long, String> clusteredWriteBehindStore = mock(ClusteredWriteBehindStore.class);
    ExecutorService executorService = new TestExecutorService();
    RecordingLoaderWriter<Long, String> cacheLoaderWriter = new RecordingLoaderWriter<>();
    OperationsCodec<Long, String> operationCodec = new OperationsCodec<>(new LongSerializer(), new StringSerializer());
    ChainResolver<Long, String> resolver = new ExpiryChainResolver<>(operationCodec, ExpiryPolicy.NO_EXPIRY);

    ClusteredWriteBehind<Long, String> clusteredWriteBehind = new ClusteredWriteBehind<>(clusteredWriteBehindStore,
                                                                                         executorService,
                                                                                         resolver,
                                                                                         cacheLoaderWriter,
                                                                                         operationCodec);
    Chain elements = makeChain(expected, operationCodec);

    when(clusteredWriteBehindStore.lock(1L)).thenReturn(new ServerStoreProxy.ChainEntry() {
      @Override
      public void append(ByteBuffer payLoad) throws TimeoutException {

      }

      @Override
      public void replaceAtHead(Chain equivalent) {

      }

      @Override
      public boolean isEmpty() {
        return elements.isEmpty();
      }

      @Override
      public int length() {
        return elements.length();
      }

      @Override
      public Iterator<Element> iterator() {
        return elements.iterator();
      }
    });

    ArgumentCaptor<Chain> chainArgumentCaptor = ArgumentCaptor.forClass(Chain.class);

    clusteredWriteBehind.flushWriteBehindQueue(null, 1L);

    Map<Long, List<String>> records = cacheLoaderWriter.getRecords();

    Map<Long, Integer> track = new HashMap<>();
    for (EventInfo event : expected) {
      if (event.track) {
        int next = track.compute(event.key, (k, v) -> v == null ? 0 : v + 1);
        assertThat(records.get(event.key).get(next), is(event.expectedValue));
      }
    }

    verify(clusteredWriteBehindStore).replaceAtHead(anyLong(), any(), chainArgumentCaptor.capture());

    Chain value = chainArgumentCaptor.getValue();
    Map<Long, String> result = convert(value, operationCodec, resolver, TIME_SOURCE);

    for (Map.Entry<Long, String> entry : result.entrySet()) {
      assertThat(entry.getValue(), is(expectedChainContents.get(entry.getKey())));
    }

    verify(clusteredWriteBehindStore).unlock(1L, false);
  }

  private Map<Long, String> convert(Chain chain, OperationsCodec<Long, String> codec,
                                    ChainResolver<Long, String> resolver, TimeSource timeSource) {
    Map<Long, String> result = new HashMap<>();
    for (Element element : chain) {
      ByteBuffer payload = element.getPayload();
      Operation<Long, String> operation = codec.decode(payload);
      Long key = operation.getKey();
      PutOperation<Long, String> opResult = resolver.applyOperation(key,
                                                          null,
                                                          operation);
      result.put(key, opResult.getValue());
    }
    return result;
  }

  private Chain makeChain(List<EventInfo> expected, OperationsCodec<Long, String> operationsCodec) {
    ChainBuilder builder = new ChainBuilder();
    for (EventInfo eventInfo : expected) {
      builder.add(operationsCodec.encode(eventInfo.operation));
    }
    return builder.build();
  }


  class TestExecutorService extends AbstractExecutorService {

    @Override
    public void shutdown() {

    }

    @Override
    public List<Runnable> shutdownNow() {
      return null;
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return false;
    }

    @Override
    public void execute(Runnable command) {
      command.run();
    }
  }

  private class EventInfo {
    private final Long key;
    private final Operation<Long, String> operation;
    private final String expectedValue;
    private final boolean track;

    private EventInfo(Long key, Operation<Long, String> operation, String expectedValue, boolean track) {
      this.key = key;
      this.operation = operation;
      this.expectedValue = expectedValue;
      this.track = track;
    }
  }
}
