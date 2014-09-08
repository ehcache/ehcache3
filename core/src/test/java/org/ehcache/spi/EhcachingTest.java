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

package org.ehcache.spi;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Alex Snaps
 */
public class EhcachingTest implements Ehcaching {

  private final static AtomicInteger instantiationCounter = new AtomicInteger(0);
  private final static AtomicInteger creationCounter = new AtomicInteger(0);

  public EhcachingTest() {
    instantiationCounter.getAndIncrement();
  }

  @Override
  public PersistentCacheManager createCacheManager(final Configuration configuration, final ServiceLocator serviceLocator) {
    creationCounter.getAndIncrement();
    return new TestCacheManager();
  }

  public static int getCreationCountAndReset() {
    return creationCounter.getAndSet(0);
  }

  public static int getInstantiationCountAndReset() {
    return instantiationCounter.getAndSet(0);
  }

  public static class TestCacheManager implements PersistentCacheManager {

    @Override
    public void destroyCache(final String alias) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public <K, V> Cache<K, V> createCache(final String alias, final CacheConfiguration<K, V> config) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public <K, V> Cache<K, V> getCache(final String alias, final Class<K> keyType, final Class<V> valueType) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void removeCache(final String alias) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException("Implement me!");
    }
  }
}
