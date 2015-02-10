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
package org.ehcache.internal.store.tiering;

import org.ehcache.Cache;
import org.ehcache.config.Eviction;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.internal.store.OnHeapStore;
import org.ehcache.internal.store.disk.DiskStorageFactory;
import org.ehcache.internal.store.disk.DiskStore;
import org.ehcache.spi.cache.Store;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author Ludovic Orban
 */
public class CacheStoreTest {

  @Test
  public void testBasics() throws Exception {
    ManualTimeSource timeSource = new ManualTimeSource();
    Comparable<Long> heapCapacityConstraint = 3L;
    Comparable<Long> diskCapacityConstraint = 6L;
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Expiry<Object, Object> expiry = Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS));
    JavaSerializationProvider serializationProvider = new JavaSerializationProvider();

    Store.Configuration<Number, CharSequence> heapConfiguration = new StoreConfigurationImpl<Number, CharSequence>(Number.class, CharSequence.class, heapCapacityConstraint, Eviction.all(), Eviction.Prioritizer.LRU, classLoader, expiry);
    Store.Configuration<Number, CharSequence> diskConfiguration = new StoreConfigurationImpl<Number, CharSequence>(Number.class, CharSequence.class, diskCapacityConstraint, Eviction.all(), Eviction.Prioritizer.LRU, classLoader, expiry);

    CachingTier<Number, CharSequence> cachingTier = new OnHeapStore<Number, CharSequence>(heapConfiguration, timeSource, false, serializationProvider.createSerializer(Number.class, ClassLoader.getSystemClassLoader()), serializationProvider.createSerializer(CharSequence.class, ClassLoader.getSystemClassLoader()));
    DiskStore<Number, CharSequence> authoritativeTier = new DiskStore<Number, CharSequence>(diskConfiguration, "CacheStoreTest", timeSource, serializationProvider.createSerializer(DiskStorageFactory.Element.class, ClassLoader.getSystemClassLoader()), serializationProvider.createSerializer(Object.class, ClassLoader.getSystemClassLoader()));
    authoritativeTier.destroy();
    authoritativeTier.create();
    authoritativeTier.init();

    CacheStore<Number, CharSequence> store = new CacheStore<Number, CharSequence>(cachingTier, authoritativeTier);

    for (int i = 0; i < 10; i++) {
      store.put(i, "#" + i);
    }
    dumpStore(store);

    authoritativeTier.flushToDisk();

    for (int i = 0; i < 3; i++) {
      store.get(i);
    }
    dumpStore(cachingTier);

    for (int i = 0; i < 3; i++) {
      store.get(i);
    }
    dumpStore(cachingTier);

    timeSource.setTime(1);
    dumpStore(cachingTier);



    store.close();
  }

  private void dumpStore(Store<Number, CharSequence> store) throws org.ehcache.exceptions.CacheAccessException {
    System.out.println("******");
    Store.Iterator<Cache.Entry<Number, Store.ValueHolder<CharSequence>>> it = store.iterator();
    while (it.hasNext()) {
      Cache.Entry<Number, Store.ValueHolder<CharSequence>> next = it.next();
      System.out.println(next.getKey() + " / " + next.getValue().value());
    }
  }


  static class ManualTimeSource implements TimeSource {
    private long time;

    public void setTime(long time) {
      this.time = time;
    }

    @Override
    public long getTimeMillis() {
      return time;
    }
  }
}
