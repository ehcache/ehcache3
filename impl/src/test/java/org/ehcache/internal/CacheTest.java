/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import java.io.Serializable;

import org.ehcache.Cache;
import org.junit.Test;
import org.ehcache.internal.cachingtier.TieredCache;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.internal.serialization.SerializationProvider;

import static org.hamcrest.core.IsNot.*;
import static org.ehcache.internal.util.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 *
 * @author Chris Dennis
 */
public class CacheTest {
  
  @Test
  public void testHeapCache() throws CacheAccessException {
    final HeapResource heapResource = new HeapResource();
    final Cache<String, String> cachingTier = heapResource.createCache(String.class, String.class);
    Cache<String, String> cache = new TieredCache<String, String>(cachingTier,
        String.class, String.class, new ServiceProvider(heapResource), new HeapResourceCacheConfiguration(1000));
    cache.put("foo", "bar");
    assertThat(cache, hasEntry("foo", "bar"));
  }

  @Test
  public void testSerializingCache() throws CacheAccessException {
    final HeapResource heapResource = new HeapResource();
    final SerializingResource serializingResource = new SerializingResource();
    final SerializationProvider serialization = new JavaSerializationProvider();
    final ServiceProvider serviceProvider = new ServiceProvider(serialization);
    serializingResource.start(serviceProvider);
    final Cache<String, String> cachingTier = serializingResource.createCache(String.class, String.class);
    Cache<String, String> cache = new TieredCache<String, String>(cachingTier, String.class, String.class, new ServiceProvider(heapResource), new HeapResourceCacheConfiguration(1000));
    cache.put("foo", "bar");
    assertThat(cache, hasEntry("foo", "bar"));
  }

  @Test
  public void testSerializingCacheWithNonSerializableType() throws CacheAccessException {
    final HeapResource heapResource = new HeapResource();
    final SerializingResource serializingResource = new SerializingResource();
    final SerializationProvider serialization = new JavaSerializationProvider();
    final ServiceProvider serviceProvider = new ServiceProvider(serialization);
    serializingResource.start(serviceProvider);
    final Cache<String, Serializable> cachingTier = serializingResource.createCache(String.class, Serializable.class);
    Cache<String, Serializable> cache = new TieredCache<String, Serializable>(cachingTier, String.class, Serializable.class, new ServiceProvider(heapResource), new HeapResourceCacheConfiguration(1000));
    try {
      cache.put("foo", new Object[] { new Object() });
      fail();
    } catch (IllegalArgumentException e) {
      //expected
    }
    assertThat(cache, not(hasKey("foo")));
  }
}
