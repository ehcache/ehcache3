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
package org.ehcache.core;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import org.ehcache.UserManagedCache;
import org.ehcache.core.spi.LifeCycled;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.core.statistics.BulkOps;

/**
 * Extension of the {@link org.ehcache.Cache} and {@link UserManagedCache} interfaces defining common methods used by
 * collaborators of {@link org.ehcache.Cache} implementations.
 * <p>
 * {@code Ehcache} users should not have to depend on this type but rely exclusively on the api types in package
 * {@code org.ehcache}.
 */
public interface InternalCache<K, V> extends UserManagedCache<K, V> {

  /**
   * BulkMethodEntries
   *
   * @return BulkMethodEntries
   */
  Map<BulkOps, LongAdder> getBulkMethodEntries();

  /**
   * Jsr107Cache
   *
   * @return Jsr107Cache
   */
  Jsr107Cache<K, V> createJsr107Cache();

  /**
   * CacheLoaderWriter
   *
   * @return CacheLoaderWriter
   */
  CacheLoaderWriter<? super K, V> getCacheLoaderWriter();

  /**
   * Add lifecycle hooks
   *
   * @param hook hook it to lifecycle
   */
  void addHook(LifeCycled hook);

}
