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

package org.ehcache.internal;

import org.ehcache.Ehcache;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.CacheProvider;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 *
 * @author Alex Snaps
 * @author Chris Dennis
 */
public class EhcacheProvider implements CacheProvider {

  @SuppressWarnings("deprecation")
  @Override
  public <K, V> Ehcache<K, V> createCache(Class<K> keyClazz, Class<V> valueClazz, ServiceConfiguration<?>... config) {
    return new HeapCache<K, V>();
  }

  @Override
  public void releaseCache(Ehcache<?, ?> resource) {
    //no-op
  }

  @Override
  public void start(ServiceConfiguration<?> config, final ServiceProvider serviceProvider) {
    //no-op
  }

  @Override
  public void stop() {
    //no-op
  }
}
