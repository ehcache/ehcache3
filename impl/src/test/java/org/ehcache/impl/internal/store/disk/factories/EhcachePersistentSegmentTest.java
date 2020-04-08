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

package org.ehcache.impl.internal.store.disk.factories;

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.impl.internal.store.disk.factories.EhcachePersistentSegmentFactory.EhcachePersistentSegment;
import org.ehcache.impl.internal.store.offheap.SwitchableEvictionAdvisor;
import org.ehcache.impl.internal.store.offheap.HeuristicConfiguration;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory.EhcacheSegment.EvictionListener;
import org.ehcache.impl.internal.store.offheap.portability.SerializerPortability;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.persistent.PersistentPortability;
import org.terracotta.offheapstore.disk.storage.FileBackedStorageEngine;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.ehcache.config.Eviction.noAdvice;
import static org.ehcache.impl.internal.store.disk.OffHeapDiskStore.persistent;
import static org.ehcache.impl.internal.spi.TestServiceProvider.providerContaining;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.terracotta.offheapstore.util.MemoryUnit.BYTES;

public class EhcachePersistentSegmentTest {

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @SuppressWarnings("unchecked")
  private EhcachePersistentSegmentFactory.EhcachePersistentSegment<String, String> createTestSegmentWithAdvisorAndListener() throws IOException {
    return createTestSegmentWithAdvisorAndListener(noAdvice(), mock(EvictionListener.class));
  }

  @SuppressWarnings("unchecked")
  private EhcachePersistentSegmentFactory.EhcachePersistentSegment<String, String> createTestSegmentWithAdvisor(EvictionAdvisor<String, String> evictionPredicate) throws IOException {
    return createTestSegmentWithAdvisorAndListener(evictionPredicate, mock(EvictionListener.class));
  }

  private EhcachePersistentSegmentFactory.EhcachePersistentSegment<String, String> createTestSegmentWithListener(EvictionListener<String, String> evictionListener) throws IOException {
    return createTestSegmentWithAdvisorAndListener(noAdvice(), evictionListener);
  }

  private EhcachePersistentSegmentFactory.EhcachePersistentSegment<String, String> createTestSegmentWithAdvisorAndListener(final EvictionAdvisor<? super String, ? super String> evictionPredicate, EvictionListener<String, String> evictionListener) throws IOException {
    try {
      HeuristicConfiguration configuration = new HeuristicConfiguration(1024 * 1024);
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining());
      MappedPageSource pageSource = new MappedPageSource(folder.newFile(), true, configuration.getMaximumSize());
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, EhcachePersistentSegmentTest.class.getClassLoader());
      Serializer<String> valueSerializer = serializationProvider.createValueSerializer(String.class, EhcachePersistentSegmentTest.class.getClassLoader());
      PersistentPortability<String> keyPortability = persistent(new SerializerPortability<>(keySerializer));
      PersistentPortability<String> elementPortability = persistent(new SerializerPortability<>(valueSerializer));
      Factory<FileBackedStorageEngine<String, String>> storageEngineFactory = FileBackedStorageEngine.createFactory(pageSource, configuration.getMaximumSize() / 10, BYTES, keyPortability, elementPortability);
      SwitchableEvictionAdvisor<String, String> wrappedEvictionAdvisor = new SwitchableEvictionAdvisor<String, String>() {

        private volatile boolean enabled = true;

        @Override
        public boolean adviseAgainstEviction(String key, String value) {
          return evictionPredicate.adviseAgainstEviction(key, value);
        }

        @Override
        public boolean isSwitchedOn() {
          return enabled;
        }

        @Override
        public void setSwitchedOn(boolean switchedOn) {
          this.enabled = switchedOn;
        }
      };
      return new EhcachePersistentSegmentFactory.EhcachePersistentSegment<>(pageSource, storageEngineFactory.newInstance(), 1, true, wrappedEvictionAdvisor, evictionListener);
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void testPutAdvisedAgainstEvictionComputesMetadata() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegmentWithAdvisor((key, value) -> {
      return "please-do-not-evict-me".equals(key);
    });
    try {
      segment.put("please-do-not-evict-me", "value");
      assertThat(segment.getMetadata("please-do-not-evict-me", EhcacheSegmentFactory.EhcacheSegment.ADVISED_AGAINST_EVICTION), is(EhcacheSegmentFactory.EhcacheSegment.ADVISED_AGAINST_EVICTION));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testPutPinnedAdvisedAgainstEvictionComputesMetadata() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegmentWithAdvisor((key, value) -> {
      return "please-do-not-evict-me".equals(key);
    });
    try {
      segment.putPinned("please-do-not-evict-me", "value");
      assertThat(segment.getMetadata("please-do-not-evict-me", EhcacheSegmentFactory.EhcacheSegment.ADVISED_AGAINST_EVICTION), is(EhcacheSegmentFactory.EhcacheSegment.ADVISED_AGAINST_EVICTION));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testAdviceAgainstEvictionPreventsEviction() throws IOException {
    EhcachePersistentSegment<String, String> segment = createTestSegmentWithAdvisorAndListener();
    try {
      assertThat(segment.evictable(1), is(true));
      assertThat(segment.evictable(EhcacheSegmentFactory.EhcacheSegment.ADVISED_AGAINST_EVICTION | 1), is(false));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testEvictionFiresEvent() throws IOException {
    @SuppressWarnings("unchecked")
    EvictionListener<String, String> evictionListener = mock(EvictionListener.class);
    EhcachePersistentSegment<String, String> segment = createTestSegmentWithListener(evictionListener);
    try {
      segment.put("key", "value");
      segment.evict(segment.getEvictionIndex(), false);
      verify(evictionListener).onEviction("key", "value");
    } finally {
      segment.destroy();
    }
  }
}
