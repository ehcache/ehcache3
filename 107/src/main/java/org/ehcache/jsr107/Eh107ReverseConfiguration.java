package org.ehcache.jsr107;

import org.ehcache.Cache;

import javax.cache.configuration.CacheEntryListenerConfiguration;

/**
 * Eh107ReverseConfiguration
 */
public class Eh107ReverseConfiguration<K, V> extends Eh107Configuration<K, V> {

  private final Cache<K, V> cache;
  private boolean managementEnabled;
  private boolean statisticsEnabled;

  public Eh107ReverseConfiguration(Cache<K, V> cache) {
    this.cache = cache;
    statisticsEnabled = true;
    managementEnabled = false;
  }

  @Override
  public boolean isReadThrough() {
    // FIXME
    return false;
  }

  @Override
  public boolean isWriteThrough() {
    // FIXME
    return false;
  }

  @Override
  public boolean isStatisticsEnabled() {
    return statisticsEnabled;
  }

  @Override
  public void setStatisticsEnabled(boolean enabled) {
    this.statisticsEnabled = enabled;
  }

  @Override
  public boolean isManagementEnabled() {
    return managementEnabled;
  }

  @Override
  public void setManagementEnabled(boolean enabled) {
    this.managementEnabled = enabled;
  }

  @Override
  public void addCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
    // FIXME: need some behavior here
  }

  @Override
  public void removeCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
    // FIXME: need some behavior here
  }

  @Override
  public Class<K> getKeyType() {
    return cache.getRuntimeConfiguration().getKeyType();
  }

  @Override
  public Class<V> getValueType() {
    return cache.getRuntimeConfiguration().getValueType();
  }

  @Override
  public boolean isStoreByValue() {
    // FIXME: need to find value from cache
    return false;
  }
}
