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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory;
import org.ehcache.impl.internal.store.offheap.portability.SerializerPortability;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;

import static org.ehcache.impl.internal.spi.TestServiceProvider.providerContaining;
import static org.ehcache.impl.internal.store.offheap.OffHeapStoreUtils.getBufferSource;
import static org.mockito.Mockito.mock;

/**
 *
 * @author cdennis
 */
public class EhcacheConcurrentOffHeapClockCacheTest extends AbstractEhcacheOffHeapBackingMapTest {

  @Override
  protected EhcacheConcurrentOffHeapClockCache<String, String> createTestSegment() {
    return createTestSegment(Eviction.noAdvice(), mock(EhcacheSegmentFactory.EhcacheSegment.EvictionListener.class));
  }

  @Override
  protected EhcacheConcurrentOffHeapClockCache<String, String> createTestSegment(EvictionAdvisor<? super String, ? super String> evictionPredicate) {
    return createTestSegment(evictionPredicate, mock(EhcacheSegmentFactory.EhcacheSegment.EvictionListener.class));
  }

  private EhcacheConcurrentOffHeapClockCache<String, String> createTestSegment(final EvictionAdvisor<? super String, ? super String> evictionPredicate, EhcacheSegmentFactory.EhcacheSegment.EvictionListener<String, String> evictionListener) {
    try {
      HeuristicConfiguration configuration = new HeuristicConfiguration(1024 * 1024);
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining());
      PageSource pageSource = new UpfrontAllocatingPageSource(getBufferSource(), configuration.getMaximumSize(), configuration.getMaximumChunkSize(), configuration.getMinimumChunkSize());
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, EhcacheConcurrentOffHeapClockCacheTest.class.getClassLoader());
      Serializer<String> valueSerializer = serializationProvider.createValueSerializer(String.class, EhcacheConcurrentOffHeapClockCacheTest.class.getClassLoader());
      Portability<String> keyPortability = new SerializerPortability<String>(keySerializer);
      Portability<String> elementPortability = new SerializerPortability<String>(valueSerializer);
      Factory<OffHeapBufferStorageEngine<String, String>> storageEngineFactory = OffHeapBufferStorageEngine.createFactory(PointerSize.INT, pageSource, configuration.getInitialSegmentTableSize(), keyPortability, elementPortability, false, true);
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
      EhcacheSegmentFactory<String, String> segmentFactory = new EhcacheSegmentFactory<String, String>(pageSource, storageEngineFactory, 0, wrappedEvictionAdvisor, evictionListener);
      return new EhcacheConcurrentOffHeapClockCache<String, String>(evictionPredicate, segmentFactory, 1);
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  protected void destroySegment(EhcacheOffHeapBackingMap<String, String> segment) {
    ((EhcacheConcurrentOffHeapClockCache<String, String>)segment).destroy();
  }

  @Override
  protected void putPinned(String key, String value, EhcacheOffHeapBackingMap<String, String> segment) {
    ((EhcacheConcurrentOffHeapClockCache<String, String>) segment).putPinned(key, value);
  }

  @Override
  protected boolean isPinned(String key, EhcacheOffHeapBackingMap<String, String> segment) {
    return ((EhcacheConcurrentOffHeapClockCache<String, String>) segment).isPinned(key);
  }

  @Override
  protected int getMetadata(String key, int mask, EhcacheOffHeapBackingMap<String, String> segment) {
    return ((EhcacheConcurrentOffHeapClockCache<String, String>) segment).getMetadata(key, mask);
  }
}
