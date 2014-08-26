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

package org.ehcache;

import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.CacheProvider;

/**
 * @author Alex Snaps
 */
public final class CacheBuilder {

  <K, V> Cache<K, V> build(ServiceProvider serviceProvider, Class<K> keyType, Class<V> valueType) {
    return serviceProvider.findService(CacheProvider.class).createCache(keyType, valueType);
  }

  public <K, V> Cache<K, V> build(Class<K> keyType, Class<V> valueType) {
    return build(new ServiceProvider(), keyType, valueType);
  }

}
