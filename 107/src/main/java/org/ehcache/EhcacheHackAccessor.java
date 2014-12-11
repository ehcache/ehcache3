package org.ehcache;

import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.writer.CacheWriter;

/**
 * EhcacheHackAccessor
 */
public final class EhcacheHackAccessor {

  private EhcacheHackAccessor() {
    // Do not instantiate
  }

  public static <K, V> CacheLoader<? super K, ? extends V> getCacheLoader(Ehcache<K, V> ehcache) {
    return ehcache.getCacheLoader();
  }

  public static <K, V> CacheWriter<? super K, ? super V> getCacheWriter(Ehcache<K, V> ehcache) {
    return ehcache.getCacheWriter();
  }

}
