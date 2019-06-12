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
package org.ehcache.impl.internal.store.tiering;

import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.docs.plugs.StringCopier;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.impl.internal.store.basic.NopStore;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.offheap.BasicOffHeapValueHolder;
import org.ehcache.spi.test.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link TieredStore}. These tests are mainly to validate that
 * <a href="https://github.com/ehcache/ehcache3/issues/1522">ehcache3#1522</a> is correctly fixed.
 * <p>
 * Only <code>putIfAbsent</code> is tested due the the time is takes to create each test. All methods that conditionally
 * modify the authoritative tier and then invalidate the caching tier are impacted.
 * <ul>
 *   <li>putIfAbsent</li>
 *   <li>remove(key, value): If the remove does nothing because the value is different, it will return KEY_PRESENT but the get will return null</li>
 *   <li>replace(key, value): Il faut avoir une valeur. Cette valeur removée mais pas encore invalidé. Ensuite un autre thread tente un replace, échoue et fait un get. Il aura l’ancienne valeur au lieu de null</li>
 *   <li>replace(key,old,new): If the replace does nothing </li>
 * </ul>
 *  They should invalidate even if hey have not modified the authoritative tier to prevent inconsistencies.
 *  <p>
 *  <b>Note:</b> In the tests below, it fails by a deadlock we are creating on purpose. In real life, we would <code>get()</code>
 *  inconsistent values instead
 */
public class TieredStoreMutatorTest {

  private static final String KEY = "KEY";
  private static final String VALUE = "VALUE";
  private static final String OTHER_VALUE = "OTHER_VALUE";

  private class AuthoritativeTierMock extends NopStore<String, String> {

    private final AtomicBoolean get = new AtomicBoolean(false);

    private final ConcurrentMap<String, String> map = new ConcurrentHashMap<>();

    @Override
    public PutStatus put(String key, String value) throws StoreAccessException {
      map.put(key, value);

      try {
        progressLatch.countDown();
        thread3Latch.await();
      } catch (InterruptedException e) {
        // ignore
      }

      return PutStatus.PUT;
    }

    @Override
    public boolean remove(String key) throws StoreAccessException {
      boolean result = map.remove(key) != null;
      try {
        progressLatch.countDown();
        thread3Latch.await();
      } catch (InterruptedException e) {
        // ignore
      }
      return result;
    }

    @Override
    public ValueHolder<String> getAndFault(String key) throws StoreAccessException {
      // First, called by Thread 1, blocks
      // Then, called by test thread, returns a value holder of null
      if (get.compareAndSet(false, true)) {
        try {
          progressLatch.countDown();
          thread1Latch.await();
        } catch (InterruptedException e) {
          // ignore
        }
      }
      return createValueHolder(map.get(key));
    }

    @Override
    public ValueHolder<String> putIfAbsent(String key, String value, Consumer<Boolean> put) throws StoreAccessException {
      return createValueHolder(map.putIfAbsent(key, value));
    }

    @Override
    public RemoveStatus remove(String key, String value) throws StoreAccessException {
      String oldValue = map.get(key);
      if(oldValue == null) {
        return RemoveStatus.KEY_MISSING;
      }
      if(value.equals(oldValue)) {
        map.remove(key);
        return RemoveStatus.REMOVED;
      }
      return RemoveStatus.KEY_PRESENT;
    }

    @Override
    public ValueHolder<String> replace(String key, String value) throws StoreAccessException {
      return createValueHolder(map.replace(key, value));
    }

    @Override
    public ReplaceStatus replace(String key, String oldValue, String newValue) throws StoreAccessException {
      String currentValue = map.get(key);
      if(currentValue == null) {
        return ReplaceStatus.MISS_NOT_PRESENT;
      }
      if(currentValue.equals(oldValue)) {
        map.replace(key, newValue);
        return ReplaceStatus.HIT;
      }
      return ReplaceStatus.MISS_PRESENT;
    }
  }

  private final AuthoritativeTier<String, String> authoritativeTier = new AuthoritativeTierMock();

  private TieredStore<String, String> tieredStore;

  private Thread thread3 = null;
  private volatile boolean failed = false;

  private final CountDownLatch progressLatch = new CountDownLatch(2);
  private final CountDownLatch thread1Latch = new CountDownLatch(1);
  private final CountDownLatch thread3Latch = new CountDownLatch(1);

  @Before
  public void setUp() throws Exception {
    // Not relevant to the test, just used to instantiate the OnHeapStore
    ResourcePools resourcePools = ResourcePoolsBuilder.newResourcePoolsBuilder()
      .heap(1, MemoryUnit.MB)
      .disk(1, MemoryUnit.GB, false)
    .build();

    // Not relevant to the test, just used to instantiate the OnHeapStore
    Store.Configuration<String, String> config = new StoreConfigurationImpl<>(String.class, String.class,
      null, getClass().getClassLoader(), ExpiryPolicyBuilder.noExpiration(), resourcePools, 0, null, null);

    // Here again, all parameters are useless, we only care about the beforeCompletingTheFault implementation
    CachingTier<String, String> cachingTier = new OnHeapStore<>(config, SystemTimeSource.INSTANCE,
      StringCopier.copier(), StringCopier.copier(), new NoopSizeOfEngine(), NullStoreEventDispatcher.
      <String, String>nullStoreEventDispatcher(), new DefaultStatisticsService());

    tieredStore = new TieredStore<>(cachingTier, authoritativeTier);
  }

  @After
  public void after() {
    releaseThreads();
  }

  @Test
  public void testPutIfAbsent() throws Exception {

    // 1. Thread 1 gets the key but found null in the on-heap backend
    // 2. Thread 1 creates a Fault and then block
    //    a. Thread 1 -> Fault.get()
    //    b. Thread 1 -> AuthoritativeTierMock.getAndFault - BLOCK
    launchThread(this::getFromTieredStore);

    // 3. Thread 2 does a put. But it hasn't invalided the on-heap yet (it blocks instead)
    //    a. Thread 2 -> TieredStore.put
    //    b. Thread 2 -> AuthoritativeTierMock.put - BLOCK
    launchThread(this::putToTieredStore);

    // At this point we have a fault with null in the caching tier and a value in the authority
    // However the fault has not yet been invalidated following the authority update
    progressLatch.await();

    // 6. Thread 3 - unblock Faults after X ms to make sure it happens after the test thread gets the fault
    launchThread3();

    // 4. Test Thread receives a value from putIfAbsent. We would expect the get to receive the same value right after
    //    a. Test Thread -> TieredStore.putIfAbsent
    //    b. Test Thread -> AuthoritativeTierMock.putIfAbsent - returns VALUE
    assertThat(putIfAbsentToTieredStore().get(), is(VALUE));

    // 5. Test Thread -> TieredStore.get()
    //    If Test Thread bugged -> Fault.get() - synchronized - blocked on the fault because thread 2 already locks the fault
    //    Else Test Thread fixed -> new Fault ... correct value
    Store.ValueHolder<String> value = getFromTieredStore();

    // These assertions will in fact work most of the time even if a failure occurred. Because as soon as the latches are
    // released by thread 3, the thread 2 will invalidate the fault
    assertThat(value, notNullValue());
    assertThat(value.get(), is(VALUE));

    // If the Test thread was blocked, Thread 3 will eventually flag the failure
    assertThat(failed, is(false));
  }

  @Test
  public void testRemoveKeyValue() throws Exception {
    // Follows the same pattern as testPutIfAbsent except that at the end, if remove returns KEY_PRESENT, we expect
    // the get to return VALUE afterwards

    launchThread(this::getFromTieredStore);

    launchThread(this::putToTieredStore);

    progressLatch.await();

    launchThread3();

    // 4. Test Thread receives KEY_PRESENT from remove. We would expect the get to receive a value right afterwards
    //    a. Test Thread -> TieredStore.remove
    //    b. Test Thread -> AuthoritativeTierMock.remove - returns KEY_PRESENT
    assertThat(removeKeyValueFromTieredStore(OTHER_VALUE), is(Store.RemoveStatus.KEY_PRESENT));

    // 5. Test Thread -> TieredStore.get()
    //    If Test Thread bugged -> Fault.get() - synchronized - blocked
    //    Else Test Thread fixed -> new Fault ... correct value
    Store.ValueHolder<String> value = getFromTieredStore();
    assertThat(value, notNullValue());
    assertThat(value.get(), is(VALUE));

    assertThat(failed, is(false));
  }

  @Test
  public void testReplaceKeyValue() throws Exception {
    // Follows the same pattern as testPutIfAbsent except that at the end, if remove returns null, we expect
    // the get to return null afterwards

    // 1. Put a value. The value is now in the authoritative tier
    putIfAbsentToTieredStore(); // using putIfAbsent instead of put here because our mock won't block on a putIfAbsent

    // 2. Thread 1 gets the key but found null in the on-heap backend
    // 3. Thread 1 creates a Fault and then block
    //    a. Thread 1 -> Fault.get()
    //    b. Thread 1 -> AuthoritativeTierMock.getAndFault - BLOCK
    launchThread(this::getFromTieredStore);

    // 3. Thread 3 does a remove. But it hasn't invalided the on-heap yet (it blocks instead)
    //    a. Thread 2 -> TieredStore.remove
    //    b. Thread 2 -> AuthoritativeTierMock.remove - BLOCK
    launchThread(this::removeKeyFromTieredStore);

    progressLatch.await();

    launchThread3();

    // 4. Test Thread receives null from replace. We would expect the get to receive the same null afterwards
    //    a. Test Thread -> TieredStore.replace
    //    b. Test Thread -> AuthoritativeTierMock.replace - returns null
    assertThat(replaceFromTieredStore(VALUE), nullValue());

    // 5. Test Thread -> TieredStore.get()
    //    If Test Thread bugged -> Fault.get() - synchronized - blocked
    //    Else Test Thread fixed -> new Fault ... correct value
    Store.ValueHolder<String> value = getFromTieredStore();
    assertThat(value, nullValue());

    assertThat(failed, is(false));
  }

  @Test
  public void testReplaceKeyOldNewValue() throws Exception {
    // Follows the same pattern as testReplaceKey

    putIfAbsentToTieredStore(); // using putIfAbsent instead of put here because our mock won't block on a putIfAbsent

    launchThread(this::getFromTieredStore);

    launchThread(this::removeKeyFromTieredStore);

    progressLatch.await();

    launchThread3();

    assertThat(replaceFromTieredStore(VALUE, OTHER_VALUE), is(Store.ReplaceStatus.MISS_NOT_PRESENT));

    // 5. Test Thread -> TieredStore.get()
    //    If Test Thread bugged -> Fault.get() - synchronized - blocked
    //    Else Test Thread fixed -> new Fault ... correct value
    Store.ValueHolder<String> value = getFromTieredStore();
    assertThat(value, nullValue());

    assertThat(failed, is(false));
  }

  private Store.ValueHolder<String> createValueHolder(String value) {
    if(value == null) {
      return null;
    }
    return new BasicOffHeapValueHolder<>(1, value, Long.MAX_VALUE, System.currentTimeMillis() - 1);
  }

  private Store.PutStatus putToTieredStore() {
    try {
      return tieredStore.put(KEY, VALUE);
    } catch (StoreAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean removeKeyFromTieredStore() {
    try {
      return tieredStore.remove(KEY);
    } catch (StoreAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private Store.ValueHolder<String> putIfAbsentToTieredStore() {
    try {
      return tieredStore.putIfAbsent(KEY, VALUE, b -> {});
    } catch (StoreAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private Store.RemoveStatus removeKeyValueFromTieredStore(String value) {
    try {
      return tieredStore.remove(KEY, value);
    } catch (StoreAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private Store.ValueHolder<String> replaceFromTieredStore(String value) {
    try {
      return tieredStore.replace(KEY, value);
    } catch (StoreAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private Store.ReplaceStatus replaceFromTieredStore(String oldValue, String newValue) {
    try {
      return tieredStore.replace(KEY, oldValue, newValue);
    } catch (StoreAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private Store.ValueHolder<String> getFromTieredStore() {
    try {
      return tieredStore.get(KEY);
    } catch (StoreAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private void launchThread3() {
    thread3 = launchThread(() -> {
      try {
        // Give time to test thread to reach blocked fault
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // ignore
      }
      failed = true;
      thread1Latch.countDown();
      thread3Latch.countDown();
    });
  }

  private Thread launchThread(Runnable runnable) {
    Thread thread = new Thread(runnable);
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  private void releaseThreads() {
    if(thread3 != null) {
      thread3.interrupt();
    }
  }
}
