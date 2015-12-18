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
import org.ehcache.config.event.CacheEventNotificationServiceConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.events.CacheEventDispatcherConfiguration;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.internal.TimeSourceConfiguration;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class EventNotificationTest {
  private static final TestTimeSource testTimeSource = new TestTimeSource();
  Listener listener1 = new Listener();
  Listener listener2 = new Listener();
  Listener listener3 = new Listener();
  AsynchronousListener asyncListener = new AsynchronousListener();

  @Test
  public void testNotificationForCacheOperations() throws InterruptedException {
    CacheEventDispatcherConfiguration notificationConfiguration = CacheEventNotificationServiceConfigurationBuilder
        .withEventProcessingQueueCount(10).build();
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(5L, EntryUnit.ENTRIES).build()).add(notificationConfiguration).buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .build(true);
    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);
    cache.getRuntimeConfiguration().registerCacheEventListener(listener1, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet
        .of(EventType.EVICTED, EventType.CREATED, EventType.UPDATED, EventType.REMOVED));
    cache.getRuntimeConfiguration().registerCacheEventListener(listener2, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet
        .of(EventType.EVICTED, EventType.CREATED, EventType.UPDATED, EventType.REMOVED));

    cache.put(1l, "1");
    assertEquals(1, listener1.created.get());
    assertEquals(0, listener1.updated.get());
    assertEquals(0, listener1.removed.get());
    assertEquals(1, listener2.created.get());
    assertEquals(0, listener2.updated.get());
    assertEquals(0, listener2.removed.get());

    Map<Long, String> entries = new HashMap<Long, String>();
    entries.put(2l, "2");
    entries.put(3l, "3");
    cache.putAll(entries);
    assertEquals(3, listener1.created.get());
    assertEquals(0, listener1.updated.get());
    assertEquals(0, listener1.removed.get());
    assertEquals(3, listener2.created.get());
    assertEquals(0, listener2.updated.get());
    assertEquals(0, listener2.removed.get());

    cache.put(1l, "01");
    assertEquals(3, listener1.created.get());
    assertEquals(1, listener1.updated.get());
    assertEquals(0, listener1.removed.get());
    assertEquals(3, listener2.created.get());
    assertEquals(1, listener2.updated.get());
    assertEquals(0, listener2.removed.get());

    cache.remove(2l);
    assertEquals(3, listener1.created.get());
    assertEquals(1, listener1.updated.get());
    assertEquals(1, listener1.removed.get());
    assertEquals(3, listener2.created.get());
    assertEquals(1, listener2.updated.get());
    assertEquals(1, listener2.removed.get());

    cache.replace(1l, "001");
    assertEquals(3, listener1.created.get());
    assertEquals(2, listener1.updated.get());
    assertEquals(1, listener1.removed.get());
    assertEquals(3, listener2.created.get());
    assertEquals(2, listener2.updated.get());
    assertEquals(1, listener2.removed.get());

    cache.replace(3l, "3", "03");
    assertEquals(3, listener1.created.get());
    assertEquals(3, listener1.updated.get());
    assertEquals(1, listener1.removed.get());
    assertEquals(3, listener2.created.get());
    assertEquals(3, listener2.updated.get());
    assertEquals(1, listener2.removed.get());

    cache.get(1L);
    assertEquals(3, listener1.created.get());
    assertEquals(3, listener1.updated.get());
    assertEquals(1, listener1.removed.get());
    assertEquals(3, listener2.created.get());
    assertEquals(3, listener2.updated.get());
    assertEquals(1, listener2.removed.get());

    cache.containsKey(1L);
    assertEquals(3, listener1.created.get());
    assertEquals(3, listener1.updated.get());
    assertEquals(1, listener1.removed.get());
    assertEquals(3, listener2.created.get());
    assertEquals(3, listener2.updated.get());
    assertEquals(1, listener2.removed.get());

    cache.put(1l, "0001");
    assertEquals(3, listener1.created.get());
    assertEquals(4, listener1.updated.get());
    assertEquals(1, listener1.removed.get());
    assertEquals(3, listener2.created.get());
    assertEquals(4, listener2.updated.get());
    assertEquals(1, listener2.removed.get());

    Set<Long> keys = new HashSet<Long>();
    keys.add(1L);
    cache.getAll(keys);
    assertEquals(3, listener1.created.get());
    assertEquals(4, listener1.updated.get());
    assertEquals(1, listener1.removed.get());
    assertEquals(3, listener2.created.get());
    assertEquals(4, listener2.updated.get());
    assertEquals(1, listener2.removed.get());

    cache.getRuntimeConfiguration().registerCacheEventListener(listener3, EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, EnumSet
        .of(EventType.EVICTED, EventType.CREATED, EventType.UPDATED, EventType.REMOVED));

    cache.replace(1l, "00001");
    assertEquals(3, listener1.created.get());
    assertEquals(5, listener1.updated.get());
    assertEquals(1, listener1.removed.get());
    assertEquals(3, listener2.created.get());
    assertEquals(5, listener2.updated.get());
    assertEquals(1, listener2.removed.get());
    assertEquals(0, listener3.created.get());
    assertEquals(1, listener3.updated.get());
    assertEquals(0, listener3.removed.get());

    cache.remove(1l);
    assertEquals(3, listener1.created.get());
    assertEquals(5, listener1.updated.get());
    assertEquals(2, listener1.removed.get());
    assertEquals(3, listener2.created.get());
    assertEquals(5, listener2.updated.get());
    assertEquals(2, listener2.removed.get());
    assertEquals(0, listener3.created.get());
    assertEquals(1, listener3.updated.get());
    assertEquals(1, listener3.removed.get());
    asyncListener.resetLatchCount(10);

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

    cacheManager.close();

    assertEquals(10, listener1.created.get());
    assertEquals(5, listener1.updated.get());
    assertEquals(2, listener1.removed.get());
    assertEquals(3, listener1.evicted.get());
    assertEquals(10, listener2.created.get());
    assertEquals(5, listener2.updated.get());
    assertEquals(2, listener2.removed.get());
    assertEquals(3, listener2.evicted.get());
    assertEquals(7, listener3.created.get());
    assertEquals(1, listener3.updated.get());
    assertEquals(1, listener3.removed.get());
    assertEquals(3, listener3.evicted.get());
    assertEquals(7, asyncListener.created.get());
    assertEquals(3, asyncListener.evicted.get());
    assertEquals(0, asyncListener.removed.get());
    assertEquals(0, asyncListener.updated.get());
  }

  @Ignore
  @Test
  public void testEventOrderForUpdateThatTriggersEviction () {
    CacheConfiguration<Long, SerializableObject> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(1L, EntryUnit.ENTRIES).offheap(1l, MemoryUnit.MB).build()).buildConfig(Long.class, SerializableObject.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .build(true);
    Cache<Long, SerializableObject> cache = cacheManager.getCache("cache", Long.class, SerializableObject.class);
    cache.getRuntimeConfiguration().registerCacheEventListener(listener1, EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, EnumSet
        .of(EventType.EVICTED, EventType.CREATED, EventType.UPDATED, EventType.REMOVED));
    SerializableObject object1 = new SerializableObject(0xAAE60);     // 700 KB
    SerializableObject object2 = new SerializableObject(0xDBBA0);     // 900 KB

    cache.put(1L, object1);
    cache.put(1L, object2);
    assertTrue(listener1.eventTypeHashMap.get(EventType.EVICTED) < listener1.eventTypeHashMap.get(EventType.UPDATED));

    cacheManager.close();
  }

  @Test
  public void testEventFiringInCacheIterator() {
    Logger logger = LoggerFactory.getLogger(Ehcache.class + "-" + "EventNotificationTest");
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withExpiry(Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.SECONDS)))
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(5L, EntryUnit.ENTRIES).build()).buildConfig(Long.class, String.class);
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .using(new TimeSourceConfiguration(testTimeSource))
        .build(true);
    testTimeSource.setTimeMillis(0);
    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);
    cache.getRuntimeConfiguration().registerCacheEventListener(listener1, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet
        .of(EventType.EXPIRED));

    cache.put(1l, "1");
    cache.put(2l, "2");
    cache.put(3l, "3");
    cache.put(4l, "4");
    cache.put(5l, "5");
    assertThat(listener1.expired.get(), is(0));
    for(Cache.Entry entry : cache) {
      logger.info("Iterating over key : ", entry.getKey());
    }

    testTimeSource.setTimeMillis(2000);
    for(Cache.Entry entry : cache) {
      logger.info("Iterating over key : ", entry.getKey());
    }

    cacheManager.close();

    assertThat(listener1.expired.get(), is(5));
  }
  
  @Test
  @Ignore("Disabled while working on moving events down to Store")
  public void testMultiThreadedSyncAsyncNotifications() throws InterruptedException {
    AsynchronousListener asyncListener = new AsynchronousListener();
    asyncListener.resetLatchCount(100);

    CacheConfiguration<Number, Number> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withExpiry(Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.SECONDS)))
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10L, EntryUnit.ENTRIES)
        )
        .buildConfig(Number.class, Number.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .using(new TimeSourceConfiguration(testTimeSource))
        .build(true);
    testTimeSource.setTimeMillis(0);
    Cache<Number, Number> cache = cacheManager.getCache("cache", Number.class, Number.class);
    cache.getRuntimeConfiguration().registerCacheEventListener(asyncListener, EventOrdering.UNORDERED, EventFiring.ASYNCHRONOUS, EnumSet
        .of(EventType.CREATED, EventType.EXPIRED));

    cache.getRuntimeConfiguration().registerCacheEventListener(listener1, EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, EnumSet
        .of(EventType.CREATED, EventType.EXPIRED));

    Thread[] operators = new Thread[10];
    for (int i = 0; i < 10; i++) {
      operators[i] = new Thread(new CachePutOperator(cache, i), "CACHE-PUT-OPERATOR_" + i);
      operators[i].start();
    }
    for (int i = 0; i < 10; i++) {
      operators[i].join();
    }
    
    testTimeSource.setTimeMillis(1000);
    operators = new Thread[10];
    for (int i = 0; i < 10; i++) {
      operators[i] = new Thread(new CacheGetOperator(cache, i), "CACHE-GET-OPERATOR_" + i);
      operators[i].start();
    }
    for (int i = 0; i < 10; i++) {
      operators[i].join();
    }
    cacheManager.close();

    assertEquals(100, listener1.created.get());
    assertEquals(10, listener1.expired.get());
    assertEquals(100, asyncListener.created.get());
    assertEquals(10, asyncListener.expired.get());
  }

  @Test
  @Ignore("Disabled while working on moving events down to Store")
  public void testMultiThreadedSyncAsyncNotificationsWithOffheap() throws InterruptedException {
    AsynchronousListener asyncListener = new AsynchronousListener();
    asyncListener.resetLatchCount(100);

    CacheConfiguration<Number, Number> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10L, EntryUnit.ENTRIES).offheap(10, MemoryUnit.MB))
        .buildConfig(Number.class, Number.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .build(true);
    Cache<Number, Number> cache = cacheManager.getCache("cache", Number.class, Number.class);
    cache.getRuntimeConfiguration().registerCacheEventListener(asyncListener, EventOrdering.UNORDERED, EventFiring.ASYNCHRONOUS, EnumSet
        .of(EventType.CREATED, EventType.EXPIRED));

    cache.getRuntimeConfiguration().registerCacheEventListener(listener1, EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, EnumSet
        .of(EventType.CREATED, EventType.EXPIRED));

    Thread[] operators = new Thread[10];
    for (int i = 0; i < 10; i++) {
      operators[i] = new Thread(new CachePutOperator(cache, i), "CACHE-PUT-OPERATOR_" + i);
      operators[i].start();
    }
    for (int i = 0; i < 10; i++) {
      operators[i].join();
    }
    cacheManager.close();

    assertEquals(100, listener1.created.get());
    assertEquals(100, asyncListener.created.get());
  }

  @Test
  public void testMultiThreadedSyncNotifications() throws InterruptedException {
    CacheConfiguration<Number, Number> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10L, EntryUnit.ENTRIES))
        .buildConfig(Number.class, Number.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .build(true);
    Cache<Number, Number> cache = cacheManager.getCache("cache", Number.class, Number.class);
    cache.getRuntimeConfiguration()
        .registerCacheEventListener(listener1, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet
            .of(EventType.CREATED, EventType.EVICTED));

    Thread[] operators = new Thread[10];
    for (int i = 1; i < 11; i++) {
      operators[i-1] = new Thread(new CachePutOperator(cache, i), "CACHE-PUT-OPERATOR_" + i);
      operators[i-1].start();
    }
    for (int i = 1; i < 11; i++) {
      operators[i-1].join();
    }
    cacheManager.close();

    assertEquals(100, listener1.created.get());
    assertEquals(90, listener1.evicted.get());
  }

  public static class Listener implements CacheEventListener<Object, Object> {
    private AtomicInteger evicted = new AtomicInteger();
    private AtomicInteger created = new AtomicInteger();
    private AtomicInteger updated = new AtomicInteger();
    private AtomicInteger removed = new AtomicInteger();
    private AtomicInteger expired = new AtomicInteger();
    private AtomicInteger eventCounter = new AtomicInteger();
    private HashMap<EventType, Integer> eventTypeHashMap = new HashMap<EventType, Integer>();

    @Override
    public void onEvent(CacheEvent<Object, Object> event) {
      Logger logger = LoggerFactory.getLogger(Ehcache.class + "-" + "EventNotificationTest");
      logger.info(event.getType().toString());
      eventTypeHashMap.put(event.getType(), eventCounter.get());
      eventCounter.getAndIncrement();
      if(event.getType() == EventType.EVICTED){
        evicted.getAndIncrement();
      }
      if(event.getType() == EventType.CREATED){
        created.getAndIncrement();
      }
      if(event.getType() == EventType.UPDATED){
        updated.getAndIncrement();
      }
      if(event.getType() == EventType.REMOVED){
        removed.getAndIncrement();
      }
      if(event.getType() == EventType.EXPIRED){
        expired.getAndIncrement();
      }
    }
  }

  public static class AsynchronousListener implements CacheEventListener<Object, Object> {
    private AtomicInteger evicted = new AtomicInteger();
    private AtomicInteger created = new AtomicInteger();
    private AtomicInteger updated = new AtomicInteger();
    private AtomicInteger removed = new AtomicInteger();
    private AtomicInteger expired = new AtomicInteger();
    private CountDownLatch latch;
    
    private void resetLatchCount(int operations) {
      this.latch = new CountDownLatch(operations);
    }

    @Override
    public void onEvent(final CacheEvent<Object, Object> event) {
      Logger logger = LoggerFactory.getLogger(EventNotificationTest.class + "-" + "EventNotificationTest");
      logger.info(event.getType().toString());
      if(event.getType() == EventType.EVICTED){
        evicted.getAndIncrement();
      }
      if(event.getType() == EventType.CREATED){
        created.getAndIncrement();
      }
      if(event.getType() == EventType.UPDATED){
        updated.getAndIncrement();
      }
      if(event.getType() == EventType.REMOVED){
        removed.getAndIncrement();
      }
      if(event.getType() == EventType.EXPIRED){
        expired.getAndIncrement();
      }
      latch.countDown();
    }
  }

  public static class SerializableObject implements Serializable {
    private int size;
    private Byte [] data;

    SerializableObject(int size) {
      this.size = size;
      this.data = new Byte[size];
    }
  }
  
  private static class CachePutOperator implements Runnable {
    Logger logger = LoggerFactory.getLogger(EventNotificationTest.class + "-" + "EventNotificationTest");
    Cache<Number, Number> cache;
    int number;
    
    CachePutOperator(Cache<Number, Number> cache, int number) {
      this.cache = cache;
      this.number = number;
    }

    @Override
    public void run() {
      for (int i = number * 100; i < (number * 100) + 10; i++) {
        cache.put(i , i);
        logger.info(Thread.currentThread().getName() + " putting " + i);
      }
    }
  }

  private static class CacheGetOperator implements Runnable {
    Cache<Number, Number> cache;
    int number;

    CacheGetOperator(Cache<Number, Number> cache, int number) {
      this.cache = cache;
      this.number = number;
    }

    @Override
    public void run() {
      for (int i = number * 100; i < (number * 100) + 10; i++) {
        cache.get(i);
      }
    }
  }
}
