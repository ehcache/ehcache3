package org.ehcache.jsr107;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;

/**
 * Eh107Configuration
 */
abstract class Eh107Configuration<K, V> implements Configuration<K, V> {
  abstract boolean isReadThrough();

  abstract boolean isWriteThrough();

  abstract boolean isStatisticsEnabled();

  abstract void setStatisticsEnabled(boolean enabled);

  abstract boolean isManagementEnabled();

  abstract void setManagementEnabled(boolean enabled);

  abstract void addCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration);

  abstract void removeCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration);
}
