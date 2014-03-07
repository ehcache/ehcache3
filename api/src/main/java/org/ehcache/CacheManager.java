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

import org.ehcache.config.CacheConfiguration;

/**
 * @author Alex Snaps
 */
public interface CacheManager {

  <K, V> Cache<K, V> addCache(String cacheName, CacheConfiguration<K, V> configuration);

  <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType);

}
