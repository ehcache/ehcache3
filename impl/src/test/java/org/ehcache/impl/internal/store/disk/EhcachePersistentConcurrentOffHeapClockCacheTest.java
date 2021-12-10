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

package org.ehcache.impl.internal.store.disk;

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.impl.internal.store.disk.factories.EhcachePersistentSegmentFactory;
import org.ehcache.impl.internal.store.offheap.AbstractEhcacheOffHeapBackingMapTest;
import org.ehcache.impl.internal.store.offheap.EhcacheOffHeapBackingMap;
import org.ehcache.impl.internal.store.offheap.SwitchableEvictionAdvisor;
import org.ehcache.impl.internal.store.offheap.HeuristicConfiguration;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory.EhcacheSegment.EvictionListener;
import org.ehcache.impl.internal.store.offheap.portability.SerializerPortability;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.junit.Rule;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.persistent.PersistentPortability;
import org.terracotta.offheapstore.disk.storage.FileBackedStorageEngine;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.ehcache.config.Eviction.noAdvice;
import static org.ehcache.impl.internal.store.disk.OffHeapDiskStore.persistent;
import static org.ehcache.impl.internal.spi.TestServiceProvider.providerContaining;
import static org.mockito.Mockito.mock;
import static org.terracotta.offheapstore.util.MemoryUnit.BYTES;

public class EhcachePersistentConcurrentOffHeapClockCacheTest extends AbstractEhcacheOffHeapBackingMapTest {

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Override
  @SuppressWarnings("unchecked")
  protected EhcachePersistentConcurrentOffHeapClockCache<String, String> createTestSegment() throws IOException {
    return createTestSegment(noAdvice(), mock(EvictionListener.class));
  }

  @Override
  @SuppressWarnings("unchecked")
  protected EhcacheOffHeapBackingMap<String, String> createTestSegment(EvictionAdvisor<? super String, ? super String> evictionPredicate) throws IOException {
    return createTestSegment(evictionPredicate, mock(EvictionListener.class));
  }

  private EhcachePersistentConcurrentOffHeapClockCache<String, String> createTestSegment(final EvictionAdvisor<? super String, ? super String> evictionPredicate, EvictionListener<String, String> evictionListener) throws IOException {
    try {
      HeuristicConfiguration configuration = new HeuristicConfiguration(1024 * 1024);
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining());
      MappedPageSource pageSource = new MappedPageSource(folder.newFile(), true, configuration.getMaximumSize());
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, EhcachePersistentConcurrentOffHeapClockCacheTest.class.getClassLoader());
      Serializer<String> valueSerializer = serializationProvider.createValueSerializer(String.class, EhcachePersistentConcurrentOffHeapClockCacheTest.class.getClassLoader());
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
      EhcachePersistentSegmentFactory<String, String> segmentFactory = new EhcachePersistentSegmentFactory<>(pageSource, storageEngineFactory, 0, wrappedEvictionAdvisor, evictionListener, true);
      return new EhcachePersistentConcurrentOffHeapClockCache<>(evictionPredicate, segmentFactory, 1);
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  protected void destroySegment(EhcacheOffHeapBackingMap<String, String> segment) {
    ((EhcachePersistentConcurrentOffHeapClockCache<String, String>)segment).destroy();
  }

  @Override
  protected void putPinned(String key, String value, EhcacheOffHeapBackingMap<String, String> segment) {
    ((EhcachePersistentConcurrentOffHeapClockCache<String, String>) segment).putPinned(key, value);
  }

  @Override
  protected boolean isPinned(String key, EhcacheOffHeapBackingMap<String, String> segment) {
    return ((EhcachePersistentConcurrentOffHeapClockCache<String, String>) segment).isPinned(key);
  }

  @Override
  protected int getMetadata(String key, int mask, EhcacheOffHeapBackingMap<String, String> segment) {
    return ((EhcachePersistentConcurrentOffHeapClockCache<String, String>) segment).getMetadata(key, mask);
  }
}
