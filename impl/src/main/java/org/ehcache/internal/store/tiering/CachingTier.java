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

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.NullaryFunction;
import org.ehcache.spi.cache.Store;

/**
 * @author Ludovic Orban
 */
public interface CachingTier<K, V> extends Store<K, V> {

  ValueHolder<V> cacheCompute(final K key, final NullaryFunction<ValueHolder<V>> source) throws CacheAccessException;

}
