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
package org.ehcache.jsr107;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;

/**
 * Eh107Configuration
 */
abstract class Eh107Configuration<K, V> implements Configuration<K, V> {

  private static final long serialVersionUID = 7324956960962454439L;

  abstract boolean isReadThrough();

  abstract boolean isWriteThrough();

  abstract boolean isStatisticsEnabled();

  abstract void setStatisticsEnabled(boolean enabled);

  abstract boolean isManagementEnabled();

  abstract void setManagementEnabled(boolean enabled);

  abstract void addCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration);

  abstract void removeCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration);
}
