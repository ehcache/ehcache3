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

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * EhcacheHackAccessor
 */
public final class EhcacheHackAccessor {

  private EhcacheHackAccessor() {
    // Do not instantiate
  }

  public static <K, V> CacheLoaderWriter<? super K, V> getCacheLoaderWriter(Ehcache<K, V> ehcache) {
    return ehcache.getCacheLoaderWriter();
  }

  public static <K, V> Jsr107Cache<K, V> getJsr107Cache(Ehcache<K, V> ehcache) {
    return ehcache.getJsr107Cache();
  }

}
