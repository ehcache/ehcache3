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

package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.Ehcache;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

/**
 * @author rism
 */
public class EventNotificationTest {
  CacheManager cacheManager;
  Listener listener1 = new Listener();
  Listener listener2 = new Listener();
  Listener listener3 = new Listener();
  AsynchronousListener asyncListener = new AsynchronousListener();
  
  @After
  public void tearDown() {
    cacheManager.close();
  }

  @Test
  public void testNotificationForCacheOperations() throws InterruptedException {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(5L, EntryUnit.ENTRIES).build()).buildConfig(Long.class, String.class);

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .build(true);
    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);
    cache.getRuntimeConfiguration().registerCacheEventListener(listener1, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet
        .of(EventType.EVICTED, EventType.CREATED, EventType.UPDATED, EventType.REMOVED));
    cache.getRuntimeConfiguration().registerCacheEventListener(listener2, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet
        .of(EventType.EVICTED, EventType.CREATED, EventType.UPDATED, EventType.REMOVED));
    
    cache.put(1l, "1");
    assertEquals(1, listener1.created);
    assertEquals(0, listener1.updated);
    assertEquals(0, listener1.removed);
    assertEquals(1, listener2.created);
    assertEquals(0, listener2.updated);
    assertEquals(0, listener2.removed);

    Map<Long, String> entries = new HashMap<Long, String>();
    entries.put(2l, "2");
    entries.put(3l, "3");
    cache.putAll(entries);
    assertEquals(3, listener1.created);
    assertEquals(0, listener1.updated);
    assertEquals(0, listener1.removed);
    assertEquals(3, listener2.created);
    assertEquals(0, listener2.updated);
    assertEquals(0, listener2.removed);

    cache.put(1l, "01");
    assertEquals(3, listener1.created);
    assertEquals(1, listener1.updated);
    assertEquals(0, listener1.removed);
    assertEquals(3, listener2.created);
    assertEquals(1, listener2.updated);
    assertEquals(0, listener2.removed);

    cache.remove(2l);
    assertEquals(3, listener1.created);
    assertEquals(1, listener1.updated);
    assertEquals(1, listener1.removed);
    assertEquals(3, listener2.created);
    assertEquals(1, listener2.updated);
    assertEquals(1, listener2.removed);

    cache.replace(1l, "001");
    assertEquals(3, listener1.created);
    assertEquals(2, listener1.updated);
    assertEquals(1, listener1.removed);
    assertEquals(3, listener2.created);
    assertEquals(2, listener2.updated);
    assertEquals(1, listener2.removed);

    cache.replace(3l, "3", "03");
    assertEquals(3, listener1.created);
    assertEquals(3, listener1.updated);
    assertEquals(1, listener1.removed);
    assertEquals(3, listener2.created);
    assertEquals(3, listener2.updated);
    assertEquals(1, listener2.removed);

    cache.get(1L);
    assertEquals(3, listener1.created);
    assertEquals(3, listener1.updated);
    assertEquals(1, listener1.removed);
    assertEquals(3, listener2.created);
    assertEquals(3, listener2.updated);
    assertEquals(1, listener2.removed);

    cache.containsKey(1L);
    assertEquals(3, listener1.created);
    assertEquals(3, listener1.updated);
    assertEquals(1, listener1.removed);
    assertEquals(3, listener2.created);
    assertEquals(3, listener2.updated);
    assertEquals(1, listener2.removed);

    cache.put(1l, "0001");
    assertEquals(3, listener1.created);
    assertEquals(4, listener1.updated);
    assertEquals(1, listener1.removed);
    assertEquals(3, listener2.created);
    assertEquals(4, listener2.updated);
    assertEquals(1, listener2.removed);

    Set<Long> keys = new HashSet<Long>();
    keys.add(1L);
    cache.getAll(keys);
    assertEquals(3, listener1.created);
    assertEquals(4, listener1.updated);
    assertEquals(1, listener1.removed);
    assertEquals(3, listener2.created);
    assertEquals(4, listener2.updated);
    assertEquals(1, listener2.removed);

    cache.getRuntimeConfiguration().registerCacheEventListener(listener3, EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, EnumSet
        .of(EventType.EVICTED, EventType.CREATED, EventType.UPDATED, EventType.REMOVED));

    cache.replace(1l, "00001");
    assertEquals(3, listener1.created);
    assertEquals(5, listener1.updated);
    assertEquals(1, listener1.removed);
    assertEquals(3, listener2.created);
    assertEquals(5, listener2.updated);
    assertEquals(1, listener2.removed);
    assertEquals(0, listener3.created);
    assertEquals(1, listener3.updated);
    assertEquals(0, listener3.removed);

    cache.remove(1l);
    assertEquals(3, listener1.created);
    assertEquals(5, listener1.updated);
    assertEquals(2, listener1.removed);
    assertEquals(3, listener2.created);
    assertEquals(5, listener2.updated);
    assertEquals(2, listener2.removed);
    assertEquals(0, listener3.created);
    assertEquals(1, listener3.updated);
    assertEquals(1, listener3.removed);

    cache.getRuntimeConfiguration().registerCacheEventListener(asyncListener, EventOrdering.ORDERED, EventFiring.ASYNCHRONOUS, EnumSet
        .of(EventType.EVICTED, EventType.CREATED, EventType.UPDATED, EventType.REMOVED));
    
    entries.clear();
    entries.put(4l, "4");
    entries.put(5l, "5");
    entries.put(6l, "6");
    entries.put(7l, "7");
    entries.put(8l, "8");
    entries.put(9l, "9");
    entries.put(10l, "10");
    
    cache.putAll(entries);
    asyncListener.latch.await();

    assertEquals(10, listener1.created);
    assertEquals(5, listener1.updated);
    assertEquals(2, listener1.removed);
    assertEquals(3, listener1.evicted);
    assertEquals(10, listener2.created);
    assertEquals(5, listener2.updated);
    assertEquals(2, listener2.removed);
    assertEquals(3, listener2.evicted);
    assertEquals(7, listener3.created);
    assertEquals(1, listener3.updated);
    assertEquals(1, listener3.removed);
    assertEquals(3, listener3.evicted);
    assertEquals(7, asyncListener.created);
    assertEquals(3, asyncListener.evicted);
    assertEquals(0, asyncListener.removed);
    assertEquals(0, asyncListener.updated);
  }
  
  public static class Listener implements CacheEventListener<Object, Object> {
    private int evicted;
    private int created;
    private int updated;
    private int removed;

    @Override
    public void onEvent(CacheEvent<Object, Object> event) {
      Logger logger = LoggerFactory.getLogger(Ehcache.class + "-" + "ListenerTest");
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
  }
  
  public static class AsynchronousListener implements CacheEventListener<Object, Object> {
    private int evicted;
    private int created;
    private int updated;
    private int removed;
    private CountDownLatch latch = new CountDownLatch(10);
    
    @Override
    public void onEvent(final CacheEvent<Object, Object> event) {
      Logger logger = LoggerFactory.getLogger(Ehcache.class + "-" + "ListenerTest");
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
      latch.countDown();            
    }
  }
}
