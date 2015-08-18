package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.Ehcache;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.ResourceType;
import org.ehcache.config.event.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author rism
 */
public class ListenerTest {
  CacheManager cacheManager;
  CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration;
  
  @After
  public void tearDown() {
    cacheManager.close();
  }
  
  @Test
  public void testListenerCacheOperations() {
    cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(ListenerObject.class, EventType.EVICTED, EventType.CREATED, EventType.UPDATED).unordered().synchronous();

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .add(cacheEventListenerConfiguration)
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(1L, EntryUnit.ENTRIES).build()).buildConfig(Long.class, String.class);

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .build(true);

    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);
    
    for(long i = 0; i < 5; i++ ){
      cache.put(i, "Hello World");
    }
    assertThat(ListenerObject.evicted, is(4));
    assertThat(ListenerObject.created, is(5));
    cache.put(4l, "Hello World");
    System.out.println("Updated : " + ListenerObject.updated);
    cache.clear();
    ListenerObject.resetEvictionCount();
    ListenerObject.resetCreationCount();
    ListenerObject.resetUpdateCount();
  }
  
  @Test
  public void testNoEventListener() {
    cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(ListenerObject.class, EventType.UPDATED, EventType.CREATED).unordered().synchronous();

    CacheConfiguration<Number, Number> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .add(cacheEventListenerConfiguration)
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(5L, EntryUnit.ENTRIES).build()).buildConfig(Number.class, Number.class);

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .build(true);

    Cache<Number, Number> cache = cacheManager.getCache("cache", Number.class, Number.class);
    
    cache.put(1l, 1l);

    Map<Number, Number> entries = new HashMap<Number, Number>();
    entries.put(2l, 2l);
    entries.put(3l, 3l);
    entries.put(4l, 4l);
    entries.put(5l, 5l);
    
    cache.putAll(entries);
    cache.putAll(entries);
    assertThat(ListenerObject.created, is(5));
    assertThat(ListenerObject.updated, is(4));
  }

  @Test
  public void testFilteredListener() throws InterruptedException {
    cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(ListenerObject.class, EventType.REMOVED, EventType.CREATED, EventType.UPDATED).unordered().synchronous();

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .add(cacheEventListenerConfiguration)
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(1L, EntryUnit.ENTRIES).build()).buildConfig(Long.class, String.class);

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .build(true);
    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);
    
    cache.put(1l, "Sooty");
    assertEquals(1, ListenerObject.created);
    assertEquals(0, ListenerObject.updated);
    assertEquals(0, ListenerObject.removed);

    Map<Long, String> entries = new HashMap<Long, String>();
    entries.put(2l, "Lucky");
    entries.put(3l, "Bryn");
    cache.putAll(entries);
    assertEquals(2, ListenerObject.created);
    assertEquals(0, ListenerObject.updated);
    assertEquals(0, ListenerObject.removed);

    cache.put(1l, "Zyn");
    assertEquals(2, ListenerObject.created);
    assertEquals(0, ListenerObject.updated);
    assertEquals(0, ListenerObject.removed);

    cache.remove(2l);
    assertEquals(2, ListenerObject.created);
    assertEquals(0, ListenerObject.updated);
    assertEquals(1, ListenerObject.removed);

    cache.replace(1l, "Fred");
    assertEquals(2, ListenerObject.created);
    assertEquals(1, ListenerObject.updated);
    assertEquals(1, ListenerObject.removed);

    cache.replace(3l, "Bryn", "Sooty");
    assertEquals(2, ListenerObject.created);
    assertEquals(2, ListenerObject.updated);
    assertEquals(1, ListenerObject.removed);

    cache.get(1L);
    assertEquals(2, ListenerObject.created);
    assertEquals(1, ListenerObject.updated);
    assertEquals(1, ListenerObject.removed);

    //containsKey is not a read for filteredListener purposes.
    cache.containsKey(1L);
    assertEquals(2, ListenerObject.created);
    assertEquals(1, ListenerObject.updated);
    assertEquals(1, ListenerObject.removed);

    //iterating should cause read events on non-expired entries
    for (Cache.Entry<Long, String> entry : cache) {
      String value = entry.getValue();
      System.out.println(value);
    }
    assertEquals(2, ListenerObject.created);
    assertEquals(1, ListenerObject.updated);
    assertEquals(1, ListenerObject.removed);

    cache.put(1l, "Pistachio");
    assertEquals(2, ListenerObject.created);
    assertEquals(3, ListenerObject.updated);
    assertEquals(1, ListenerObject.removed);

    Set<Long> keys = new HashSet<Long>();
    keys.add(1L);
    cache.getAll(keys);
    assertEquals(2, ListenerObject.created);
    assertEquals(3, ListenerObject.updated);
    assertEquals(1, ListenerObject.removed);

    cache.replace(1l, "Prince");
    assertEquals(2, ListenerObject.created);
    assertEquals(4, ListenerObject.updated);
    assertEquals(1, ListenerObject.removed);

    cache.remove(1l);
    assertEquals(2, ListenerObject.created);
    assertEquals(4, ListenerObject.updated);
    assertEquals(2, ListenerObject.removed);
  }

  public static class ListenerObject implements CacheEventListener<Object, Object> {
    private static int evicted;
    private static int created;
    private static int updated;
    private static int removed;
    
    @Override
    public void onEvent(CacheEvent<Object, Object> event) {
      Logger logger = LoggerFactory.getLogger(Ehcache.class + "-" + "GettingStarted");
      logger.info(event.getType().toString());
      if(event.getType() == EventType.EVICTED){
        evicted++;
      }
      if(event.getType() == EventType.CREATED){
        created++;
      }
      if(event.getType() == EventType.UPDATED){
        updated++;
      }
      if(event.getType() == EventType.REMOVED){
        removed++;
      }
    }
    public static void resetEvictionCount() {
      evicted = 0;
    }
    public static void resetCreationCount() {
      created = 0;
    }
    public static void resetUpdateCount() {
      updated = 0;
    }
    public static void resetRemoveCount() {
      removed = 0;
    }
  }
}
