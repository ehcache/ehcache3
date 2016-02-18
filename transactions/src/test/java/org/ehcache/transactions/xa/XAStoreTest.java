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
package org.ehcache.transactions.xa;

import org.ehcache.Cache;
import org.ehcache.core.config.store.StoreConfigurationImpl;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.impl.internal.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.offheap.MemorySizeParser;
import org.ehcache.impl.internal.store.offheap.OffHeapStore;
import org.ehcache.impl.internal.store.tiering.CacheStore;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.impl.internal.store.offheap.OffHeapStoreLifecycleHelper;
import org.ehcache.core.spi.cache.Store;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.impl.internal.spi.copy.DefaultCopyProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.transactions.xa.journal.Journal;
import org.ehcache.transactions.xa.journal.TransientJournal;
import org.ehcache.transactions.xa.txmgr.NullXAResourceRegistry;
import org.ehcache.transactions.xa.txmgr.TransactionManagerWrapper;
import org.ehcache.transactions.xa.utils.JavaSerializer;
import org.ehcache.transactions.xa.utils.TestXid;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * @author Ludovic Orban
 */
public class XAStoreTest {

  private final TestTransactionManager testTransactionManager = new TestTransactionManager();

  @Test
  public void testSimpleGetPutRemove() throws Exception {
    String uniqueXAResourceId = "testSimpleGetPutRemove";
    TransactionManagerWrapper transactionManagerWrapper = new TransactionManagerWrapper(testTransactionManager, new NullXAResourceRegistry());
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    CopyProvider copyProvider = new DefaultCopyProvider(new DefaultCopyProviderConfiguration());
    Copier<Long> keyCopier = copyProvider.createKeyCopier(Long.class, keySerializer);
    Copier<SoftLock> valueCopier = copyProvider.createValueCopier(SoftLock.class, valueSerializer);
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class,
        null, classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
        0, keySerializer, valueSerializer);
    TestTimeSource testTimeSource = new TestTimeSource();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), NullStoreEventDispatcher.<Long, SoftLock>nullStoreEventDispatcher());
    Journal<Long> journal = new TransientJournal<Long>();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(Long.class, String.class, onHeapStore, transactionManagerWrapper, testTimeSource, journal, uniqueXAResourceId);

    testTransactionManager.begin();
    {
      assertThat(xaStore.get(1L), is(nullValue()));

      xaStore.put(1L, "one");

      assertThat(xaStore.get(1L).value(), equalTo("one"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      xaStore.remove(1L);

      assertThat(xaStore.get(1L), is(nullValue()));
    }
    testTransactionManager.rollback();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      xaStore.put(1L, "un");

      assertThat(xaStore.get(1L).value(), equalTo("un"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "un");
  }

  @Test
  public void testIterate() throws Exception {
    String uniqueXAResourceId = "testIterate";
    TransactionManagerWrapper transactionManagerWrapper = new TransactionManagerWrapper(testTransactionManager, new NullXAResourceRegistry());
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    CopyProvider copyProvider = new DefaultCopyProvider(new DefaultCopyProviderConfiguration());
    Copier<Long> keyCopier = copyProvider.createKeyCopier(Long.class, keySerializer);
    Copier<SoftLock> valueCopier = copyProvider.createValueCopier(SoftLock.class, valueSerializer);
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class,
        null, classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
        0, keySerializer, valueSerializer);
    TestTimeSource testTimeSource = new TestTimeSource();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), NullStoreEventDispatcher.<Long, SoftLock>nullStoreEventDispatcher());
    Journal<Long> journal = new TransientJournal<Long>();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(Long.class, String.class, onHeapStore, transactionManagerWrapper, testTimeSource, journal, uniqueXAResourceId);

    testTransactionManager.begin();
    {
      xaStore.put(1L, "one");
      xaStore.put(2L, "two");
      xaStore.put(3L, "three");
    }
    testTransactionManager.commit();

    testTransactionManager.begin();
    {
      xaStore.put(0L, "zero");
      xaStore.put(1L, "un");
      xaStore.put(2L, "two");
      xaStore.remove(3L);

      Map<Long, String> iterated = new HashMap<Long, String>();
      Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator = xaStore.iterator();
      while (iterator.hasNext()) {
        Cache.Entry<Long, Store.ValueHolder<String>> next = iterator.next();
        iterated.put(next.getKey(), next.getValue().value());
      }
      assertThat(iterated.size(), is(3));
      assertThat(iterated.get(0L), equalTo("zero"));
      assertThat(iterated.get(1L), equalTo("un"));
      assertThat(iterated.get(2L), equalTo("two"));
    }
    testTransactionManager.commit();

    testTransactionManager.begin();
    {
      Map<Long, String> iterated = new HashMap<Long, String>();
      Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator = xaStore.iterator();
      while (iterator.hasNext()) {
        Cache.Entry<Long, Store.ValueHolder<String>> next = iterator.next();
        iterated.put(next.getKey(), next.getValue().value());
      }
      assertThat(iterated.size(), is(3));
      assertThat(iterated.get(0L), equalTo("zero"));
      assertThat(iterated.get(1L), equalTo("un"));
      assertThat(iterated.get(2L), equalTo("two"));
    }
    testTransactionManager.commit();

    Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator;
    testTransactionManager.begin();
    {
      iterator = xaStore.iterator();
      iterator.next();
    }
    testTransactionManager.commit();

    // cannot use iterator outside of tx context
    try {
      iterator.hasNext();
      fail();
    } catch (XACacheException e) {
      // expected
    }
    try {
      iterator.next();
      fail();
    } catch (XACacheException e) {
      // expected
    }

    // cannot use iterator outside of original tx context
    testTransactionManager.begin();
    {
      try {
        iterator.hasNext();
        fail();
      } catch (IllegalStateException e) {
        // expected
      }
      try {
        iterator.next();
        fail();
      } catch (IllegalStateException e) {
        // expected
      }
    }
    testTransactionManager.commit();
  }

  @Test
  public void testCompute() throws Exception {
    String uniqueXAResourceId = "testCompute";
    TransactionManagerWrapper transactionManagerWrapper = new TransactionManagerWrapper(testTransactionManager, new NullXAResourceRegistry());
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    CopyProvider copyProvider = new DefaultCopyProvider(new DefaultCopyProviderConfiguration());
    Copier<Long> keyCopier = copyProvider.createKeyCopier(Long.class, keySerializer);
    Copier<SoftLock> valueCopier = copyProvider.createValueCopier(SoftLock.class, valueSerializer);
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
        0, keySerializer, valueSerializer);
    TestTimeSource testTimeSource = new TestTimeSource();
    StoreEventDispatcher<Long, SoftLock> eventDispatcher = NullStoreEventDispatcher.<Long, SoftLock>nullStoreEventDispatcher();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock> offHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class,
        null, classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
        0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = (OffHeapStore) new OffHeapStore<Long, SoftLock>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser.parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    CacheStore<Long, SoftLock<String>> cacheStore = new CacheStore<Long, SoftLock<String>>(onHeapStore, offHeapStore);
    Journal<Long> journal = new TransientJournal<Long>();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(Long.class, String.class, cacheStore, transactionManagerWrapper, testTimeSource, journal, uniqueXAResourceId);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          assertThat(aLong, is(1L));
          assertThat(s, is(nullValue()));
          return "one";
        }
      });
      Assert.assertThat(computed1.value(), equalTo("one"));
      Store.ValueHolder<String> computed2 = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          assertThat(aLong, is(1L));
          assertThat(s, equalTo("one"));
          return "un";
        }
      });
      Assert.assertThat(computed2.value(), equalTo("un"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "un");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          assertThat(aLong, is(1L));
          assertThat(s, equalTo("un"));
          return "eins";
        }
      });
      Assert.assertThat(computed.value(), equalTo("eins"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "eins");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          assertThat(aLong, is(1L));
          assertThat(s, equalTo("eins"));
          return null;
        }
      });
      Assert.assertThat(computed, is(nullValue()));
    }
    testTransactionManager.rollback();

    assertMapping(xaStore, 1L, "eins");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          assertThat(aLong, is(1L));
          assertThat(s, equalTo("eins"));
          return null;
        }
      });
      Assert.assertThat(computed1, is(nullValue()));
      Store.ValueHolder<String> computed2 = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          assertThat(aLong, is(1L));
          assertThat(s, is(nullValue()));
          return null;
        }
      });
      Assert.assertThat(computed2, is(nullValue()));
      Store.ValueHolder<String> computed3 = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          assertThat(aLong, is(1L));
          assertThat(s, is(nullValue()));
          return "uno";
        }
      });
      Assert.assertThat(computed3.value(), equalTo("uno"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "uno");

    testTransactionManager.begin();
    {
      xaStore.remove(1L);
    }
    testTransactionManager.commit();

    testTransactionManager.begin();
    {
      assertThat(xaStore.containsKey(1L), is(false));
      xaStore.put(1L, "uno");
      assertThat(xaStore.containsKey(1L), is(true));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "uno");

    testTransactionManager.begin();
    {
      assertThat(xaStore.containsKey(1L), is(true));
      xaStore.remove(1L);
      assertThat(xaStore.containsKey(1L), is(false));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    OffHeapStoreLifecycleHelper.close(offHeapStore);
  }

  @Test
  public void testComputeIfAbsent() throws Exception {
    String uniqueXAResourceId = "testComputeIfAbsent";
    TransactionManagerWrapper transactionManagerWrapper = new TransactionManagerWrapper(testTransactionManager, new NullXAResourceRegistry());
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    CopyProvider copyProvider = new DefaultCopyProvider(new DefaultCopyProviderConfiguration());
    Copier<Long> keyCopier = copyProvider.createKeyCopier(Long.class, keySerializer);
    Copier<SoftLock> valueCopier = copyProvider.createValueCopier(SoftLock.class, valueSerializer);
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
        0, keySerializer, valueSerializer);
    TestTimeSource testTimeSource = new TestTimeSource();
    StoreEventDispatcher<Long, SoftLock> eventDispatcher = NullStoreEventDispatcher.<Long, SoftLock>nullStoreEventDispatcher();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock> offHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
        0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = (OffHeapStore) new OffHeapStore<Long, SoftLock>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser.parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    CacheStore<Long, SoftLock<String>> cacheStore = new CacheStore<Long, SoftLock<String>>(onHeapStore, offHeapStore);
    Journal<Long> journal = new TransientJournal<Long>();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(Long.class, String.class, cacheStore, transactionManagerWrapper, testTimeSource, journal, uniqueXAResourceId);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.computeIfAbsent(1L, new Function<Long, String>() {
        @Override
        public String apply(Long aLong) {
          Assert.assertThat(aLong, is(1L));
          return "one";
        }
      });
      Assert.assertThat(computed1.value(), equalTo("one"));
      Store.ValueHolder<String> computed2 = xaStore.computeIfAbsent(1L, new Function<Long, String>() {
        @Override
        public String apply(Long aLong) {
          fail("should not be absent");
          throw new AssertionError();
        }
      });
      Assert.assertThat(computed2, is(nullValue()));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.computeIfAbsent(1L, new Function<Long, String>() {
        @Override
        public String apply(Long aLong) {
          fail("should not be absent");
          throw new AssertionError();
        }
      });
      Assert.assertThat(computed1.value(), equalTo("one"));

      xaStore.remove(1L);

      Store.ValueHolder<String> computed2 = xaStore.computeIfAbsent(1L, new Function<Long, String>() {
        @Override
        public String apply(Long aLong) {
          Assert.assertThat(aLong, is(1L));
          return "un";
        }
      });
      Assert.assertThat(computed2.value(), equalTo("un"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "un");

    OffHeapStoreLifecycleHelper.close(offHeapStore);
  }

  @Test
  public void testComputeIfPresent() throws Exception {
    String uniqueXAResourceId = "testComputeIfPresent";
    TransactionManagerWrapper transactionManagerWrapper = new TransactionManagerWrapper(testTransactionManager, new NullXAResourceRegistry());
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    CopyProvider copyProvider = new DefaultCopyProvider(new DefaultCopyProviderConfiguration());
    Copier<Long> keyCopier = copyProvider.createKeyCopier(Long.class, keySerializer);
    Copier<SoftLock> valueCopier = copyProvider.createValueCopier(SoftLock.class, valueSerializer);
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
        0, keySerializer, valueSerializer);
    TestTimeSource testTimeSource = new TestTimeSource();
    StoreEventDispatcher<Long, SoftLock> eventDispatcher = NullStoreEventDispatcher.<Long, SoftLock>nullStoreEventDispatcher();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock> offHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
        0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = (OffHeapStore) new OffHeapStore<Long, SoftLock>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser.parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    CacheStore<Long, SoftLock<String>> cacheStore = new CacheStore<Long, SoftLock<String>>(onHeapStore, offHeapStore);
    Journal<Long> journal = new TransientJournal<Long>();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(Long.class, String.class, cacheStore, transactionManagerWrapper, testTimeSource, journal, uniqueXAResourceId);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.computeIfPresent(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          fail("should not be present");
          throw new AssertionError();
        }
      });
      Assert.assertThat(computed1, is(nullValue()));

      xaStore.put(1L, "one");

      Store.ValueHolder<String> computed2 = xaStore.computeIfPresent(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          assertThat(aLong, is(1L));
          assertThat(s, equalTo("one"));
          return "un";
        }
      });
      Assert.assertThat(computed2.value(), equalTo("un"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "un");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.computeIfPresent(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          assertThat(aLong, is(1L));
          assertThat(s, equalTo("un"));
          return "one";
        }
      });
      Assert.assertThat(computed1.value(), equalTo("one"));

      xaStore.remove(1L);

      Store.ValueHolder<String> computed2 = xaStore.computeIfPresent(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          fail("should not be present");
          throw new AssertionError();
        }
      });
      Assert.assertThat(computed2, is(nullValue()));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    OffHeapStoreLifecycleHelper.close(offHeapStore);
  }

  @Test
  public void testExpiry() throws Exception {
    String uniqueXAResourceId = "testExpiry";
    TransactionManagerWrapper transactionManagerWrapper = new TransactionManagerWrapper(testTransactionManager, new NullXAResourceRegistry());
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    CopyProvider copyProvider = new DefaultCopyProvider(new DefaultCopyProviderConfiguration());
    Copier<Long> keyCopier = copyProvider.createKeyCopier(Long.class, keySerializer);
    Copier<SoftLock> valueCopier = copyProvider.createValueCopier(SoftLock.class, valueSerializer);
    Expiry<Object, Object> expiry = Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.SECONDS));
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
        0, keySerializer, valueSerializer);
    TestTimeSource testTimeSource = new TestTimeSource();
    StoreEventDispatcher<Long, SoftLock> eventDispatcher = NullStoreEventDispatcher.<Long, SoftLock>nullStoreEventDispatcher();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock> offHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
        0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = (OffHeapStore) new OffHeapStore<Long, SoftLock>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser.parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    CacheStore<Long, SoftLock<String>> cacheStore = new CacheStore<Long, SoftLock<String>>(onHeapStore, offHeapStore);
    Journal<Long> journal = new TransientJournal<Long>();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(Long.class, String.class, cacheStore, transactionManagerWrapper, testTimeSource, journal, uniqueXAResourceId);

    testTransactionManager.begin();
    {
      xaStore.put(1L, "one");
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "one");

    testTimeSource.advanceTime(2000);

    assertMapping(xaStore, 1L, null);
  }

  @Test
  public void testExpiryCreateException() throws Exception {
    String uniqueXAResourceId = "testExpiryCreateException";
    TransactionManagerWrapper transactionManagerWrapper = new TransactionManagerWrapper(testTransactionManager, new NullXAResourceRegistry());
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    CopyProvider copyProvider = new DefaultCopyProvider(new DefaultCopyProviderConfiguration());
    Copier<Long> keyCopier = copyProvider.createKeyCopier(Long.class, keySerializer);
    Copier<SoftLock> valueCopier = copyProvider.createValueCopier(SoftLock.class, valueSerializer);
    Expiry<Object, Object> expiry = new Expiry<Object, Object>() {

      @Override
      public Duration getExpiryForCreation(Object key, Object value) {
        throw new RuntimeException();
      }

      @Override
      public Duration getExpiryForAccess(Object key, Object value) {
        throw new AssertionError();
      }

      @Override
      public Duration getExpiryForUpdate(Object key, Object oldValue, Object newValue) {
        throw new AssertionError();
      }
    };
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
        0, keySerializer, valueSerializer);
    TestTimeSource testTimeSource = new TestTimeSource();
    StoreEventDispatcher<Long, SoftLock> eventDispatcher = NullStoreEventDispatcher.<Long, SoftLock>nullStoreEventDispatcher();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock> offHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
        0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = (OffHeapStore) new OffHeapStore<Long, SoftLock>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser.parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    CacheStore<Long, SoftLock<String>> cacheStore = new CacheStore<Long, SoftLock<String>>(onHeapStore, offHeapStore);
    Journal<Long> journal = new TransientJournal<Long>();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(Long.class, String.class, cacheStore, transactionManagerWrapper, testTimeSource, journal, uniqueXAResourceId);

    testTransactionManager.begin();
    xaStore.put(1L, "one");
    testTransactionManager.commit();
    assertMapping(xaStore, 1L, null);
  }

  @Test
  public void testExpiryAccessException() throws Exception {
    String uniqueXAResourceId = "testExpiryAccessException";
    final TestTimeSource testTimeSource = new TestTimeSource();
    TransactionManagerWrapper transactionManagerWrapper = new TransactionManagerWrapper(testTransactionManager, new NullXAResourceRegistry());
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    CopyProvider copyProvider = new DefaultCopyProvider(new DefaultCopyProviderConfiguration());
    Copier<Long> keyCopier = copyProvider.createKeyCopier(Long.class, keySerializer);
    Copier<SoftLock> valueCopier = copyProvider.createValueCopier(SoftLock.class, valueSerializer);
    Expiry<Object, Object> expiry = new Expiry<Object, Object>() {

      @Override
      public Duration getExpiryForCreation(Object key, Object value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForAccess(Object key, Object value) {
        if (testTimeSource.getTimeMillis() > 0) {
          throw new RuntimeException();
        }
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForUpdate(Object key, Object oldValue, Object newValue) {
        return Duration.FOREVER;
      }
    };
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
        0, keySerializer, valueSerializer);
    StoreEventDispatcher<Long, SoftLock> eventDispatcher = NullStoreEventDispatcher.<Long, SoftLock>nullStoreEventDispatcher();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock> offHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
        0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = (OffHeapStore) new OffHeapStore<Long, SoftLock>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser.parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    CacheStore<Long, SoftLock<String>> cacheStore = new CacheStore<Long, SoftLock<String>>(onHeapStore, offHeapStore);
    Journal<Long> journal = new TransientJournal<Long>();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(Long.class, String.class, cacheStore, transactionManagerWrapper, testTimeSource, journal, uniqueXAResourceId);

    testTransactionManager.begin();
    xaStore.put(1L, "one");
    testTransactionManager.commit();
    
    testTimeSource.advanceTime(1000);
    testTransactionManager.begin();
    assertNull(xaStore.get(1L));
    testTransactionManager.commit();
  }

  @Test
  public void testExpiryUpdateException() throws Exception{
    String uniqueXAResourceId = "testExpiryUpdateException";
    final TestTimeSource testTimeSource = new TestTimeSource();
    TransactionManagerWrapper transactionManagerWrapper = new TransactionManagerWrapper(testTransactionManager, new NullXAResourceRegistry());
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    CopyProvider copyProvider = new DefaultCopyProvider(new DefaultCopyProviderConfiguration());
    Copier<Long> keyCopier = copyProvider.createKeyCopier(Long.class, keySerializer);
    Copier<SoftLock> valueCopier = copyProvider.createValueCopier(SoftLock.class, valueSerializer);
    Expiry<Object, Object> expiry = new Expiry<Object, Object>() {

      @Override
      public Duration getExpiryForCreation(Object key, Object value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForAccess(Object key, Object value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForUpdate(Object key, Object oldValue, Object newValue) {
        if (testTimeSource.getTimeMillis() > 0) {
          throw new RuntimeException();
        }
        return Duration.FOREVER;
      }
    };
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
        0, keySerializer, valueSerializer);
    StoreEventDispatcher<Long, SoftLock> eventDispatcher = NullStoreEventDispatcher.<Long, SoftLock>nullStoreEventDispatcher();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock> offHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
        0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = (OffHeapStore) new OffHeapStore<Long, SoftLock>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser.parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    CacheStore<Long, SoftLock<String>> cacheStore = new CacheStore<Long, SoftLock<String>>(onHeapStore, offHeapStore);
    Journal<Long> journal = new TransientJournal<Long>();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(Long.class, String.class, cacheStore, transactionManagerWrapper, testTimeSource, journal, uniqueXAResourceId);

    testTransactionManager.begin();
    xaStore.put(1L, "one");
    xaStore.get(1L);
    testTransactionManager.commit();

    testTimeSource.advanceTime(1000);
    testTransactionManager.begin();
    xaStore.put(1L, "two");
    testTransactionManager.commit();
    assertMapping(xaStore, 1L, null);
  }

  @Test
  public void testBulkCompute() throws Exception {
    String uniqueXAResourceId = "testBulkCompute";
    TransactionManagerWrapper transactionManagerWrapper = new TransactionManagerWrapper(testTransactionManager, new NullXAResourceRegistry());
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    CopyProvider copyProvider = new DefaultCopyProvider(new DefaultCopyProviderConfiguration());
    Copier<Long> keyCopier = copyProvider.createKeyCopier(Long.class, keySerializer);
    Copier<SoftLock> valueCopier = copyProvider.createValueCopier(SoftLock.class, valueSerializer);
    Expiry<Object, Object> expiry = Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.SECONDS));
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
        0, keySerializer, valueSerializer);
    TestTimeSource testTimeSource = new TestTimeSource();
    StoreEventDispatcher<Long, SoftLock> eventDispatcher = NullStoreEventDispatcher.<Long, SoftLock>nullStoreEventDispatcher();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock> offHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
        0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = (OffHeapStore) new OffHeapStore<Long, SoftLock>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser.parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    CacheStore<Long, SoftLock<String>> cacheStore = new CacheStore<Long, SoftLock<String>>(onHeapStore, offHeapStore);
    Journal<Long> journal = new TransientJournal<Long>();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(Long.class, String.class, cacheStore, transactionManagerWrapper, testTimeSource, journal, uniqueXAResourceId);

    testTransactionManager.begin();
    {
      Map<Long, Store.ValueHolder<String>> computedMap = xaStore.bulkCompute(asSet(1L, 2L, 3L), new Function<Iterable<? extends Map.Entry<? extends Long, ? extends String>>, Iterable<? extends Map.Entry<? extends Long, ? extends String>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends Long, ? extends String>> apply(Iterable<? extends Map.Entry<? extends Long, ? extends String>> entries) {
          Map<Long, String> result = new HashMap<Long, String>();
          for (Map.Entry<? extends Long, ? extends String> entry : entries) {
            Long key = entry.getKey();
            String value = entry.getValue();
            Assert.assertThat(value, is(nullValue()));
            result.put(key, "stuff#" + key);
          }
          return result.entrySet();
        }
      });

      assertThat(computedMap.size(), is(3));
      assertThat(computedMap.get(1L).value(), equalTo("stuff#1"));
      assertThat(computedMap.get(2L).value(), equalTo("stuff#2"));
      assertThat(computedMap.get(3L).value(), equalTo("stuff#3"));


      computedMap = xaStore.bulkCompute(asSet(0L, 1L, 3L), new Function<Iterable<? extends Map.Entry<? extends Long, ? extends String>>, Iterable<? extends Map.Entry<? extends Long, ? extends String>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends Long, ? extends String>> apply(Iterable<? extends Map.Entry<? extends Long, ? extends String>> entries) {
          Map<Long, String> result = new HashMap<Long, String>();
          for (Map.Entry<? extends Long, ? extends String> entry : entries) {
            Long key = entry.getKey();
            String value = entry.getValue();

            switch (key.intValue()) {
              case 0:
                assertThat(value, is(nullValue()));
                break;
              case 1:
              case 3:
                assertThat(value, equalTo("stuff#" + key));
                break;
            }

            if (key != 3L) {
              result.put(key, "otherStuff#" + key);
            } else {
              result.put(key, null);
            }
          }
          return result.entrySet();
        }
      });

      assertThat(computedMap.size(), is(3));
      assertThat(computedMap.get(0L).value(), equalTo("otherStuff#0"));
      assertThat(computedMap.get(1L).value(), equalTo("otherStuff#1"));
      assertThat(computedMap.get(3L), is(nullValue()));
    }
    testTransactionManager.commit();

    assertSize(xaStore, 3);
    assertMapping(xaStore, 0L, "otherStuff#0");
    assertMapping(xaStore, 1L, "otherStuff#1");
    assertMapping(xaStore, 2L, "stuff#2");
  }

  @Test
  public void testBulkComputeIfAbsent() throws Exception {
    String uniqueXAResourceId = "testBulkComputeIfAbsent";
    TransactionManagerWrapper transactionManagerWrapper = new TransactionManagerWrapper(testTransactionManager, new NullXAResourceRegistry());
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    CopyProvider copyProvider = new DefaultCopyProvider(new DefaultCopyProviderConfiguration());
    Copier<Long> keyCopier = copyProvider.createKeyCopier(Long.class, keySerializer);
    Copier<SoftLock> valueCopier = copyProvider.createValueCopier(SoftLock.class, valueSerializer);
    Expiry<Object, Object> expiry = Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.SECONDS));
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
        0, keySerializer, valueSerializer);
    TestTimeSource testTimeSource = new TestTimeSource();
    StoreEventDispatcher<Long, SoftLock> eventDispatcher = NullStoreEventDispatcher.<Long, SoftLock>nullStoreEventDispatcher();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock> offHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null,
        classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
        0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = (OffHeapStore) new OffHeapStore<Long, SoftLock>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser.parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    CacheStore<Long, SoftLock<String>> cacheStore = new CacheStore<Long, SoftLock<String>>(onHeapStore, offHeapStore);
    Journal<Long> journal = new TransientJournal<Long>();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(Long.class, String.class, cacheStore, transactionManagerWrapper, testTimeSource, journal, uniqueXAResourceId);

    testTransactionManager.begin();
    {
      Map<Long, Store.ValueHolder<String>> computedMap = xaStore.bulkComputeIfAbsent(asSet(1L, 2L, 3L), new Function<Iterable<? extends Long>, Iterable<? extends Map.Entry<? extends Long, ? extends String>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends Long, ? extends String>> apply(Iterable<? extends Long> keys) {
          Map<Long, String> result = new HashMap<Long, String>();
          for (Long key : keys) {
            result.put(key, "stuff#" + key);
          }
          return result.entrySet();
        }
      });

      assertThat(computedMap.size(), is(3));
      assertThat(computedMap.get(1L).value(), equalTo("stuff#1"));
      assertThat(computedMap.get(2L).value(), equalTo("stuff#2"));
      assertThat(computedMap.get(3L).value(), equalTo("stuff#3"));

      computedMap = xaStore.bulkComputeIfAbsent(asSet(0L, 1L, 3L), new Function<Iterable<? extends Long>, Iterable<? extends Map.Entry<? extends Long, ? extends String>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends Long, ? extends String>> apply(Iterable<? extends Long> keys) {
          Map<Long, String> result = new HashMap<Long, String>();
          for (Long key : keys) {
            switch (key.intValue()) {
              case 0:
                result.put(key, "otherStuff#" + key);
                break;
              case 1:
              case 3:
                fail("key " + key + " should not be absent");
                break;
            }
          }
          return result.entrySet();
        }
      });

      assertThat(computedMap.size(), is(3));
      assertThat(computedMap.get(0L).value(), equalTo("otherStuff#0"));
      assertThat(computedMap.get(1L), is(nullValue()));
      assertThat(computedMap.get(3L), is(nullValue()));
    }
    testTransactionManager.commit();

    assertSize(xaStore, 4);
    assertMapping(xaStore, 0L, "otherStuff#0");
    assertMapping(xaStore, 1L, "stuff#1");
    assertMapping(xaStore, 2L, "stuff#2");
    assertMapping(xaStore, 3L, "stuff#3");
  }

  private Set<Long> asSet(Long... longs) {
    return new HashSet<Long>(Arrays.asList(longs));
  }

  private void assertMapping(XAStore<Long, String> xaStore, long key, String value) throws Exception {
    testTransactionManager.begin();

    Store.ValueHolder<String> valueHolder = xaStore.get(key);
    if (value != null) {
      assertThat(valueHolder.value(), equalTo(value));
    } else {
      assertThat(valueHolder, is(nullValue()));
    }

    testTransactionManager.commit();
  }

  private void assertSize(XAStore<Long, String> xaStore, int expectedSize) throws Exception {
    testTransactionManager.begin();

    int counter = 0;
    Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator = xaStore.iterator();
    while (iterator.hasNext()) {
      Cache.Entry<Long, Store.ValueHolder<String>> next = iterator.next();
      counter++;
    }
    Assert.assertThat(counter, is(expectedSize));

    testTransactionManager.commit();
  }

  static class TestTransactionManager implements TransactionManager {

    volatile TestTransaction currentTransaction;
    final AtomicLong gtridGenerator = new AtomicLong();

    @Override
    public void begin() throws NotSupportedException, SystemException {
      currentTransaction = new TestTransaction(gtridGenerator.incrementAndGet());
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
      currentTransaction.commit();
      currentTransaction = null;
    }

    @Override
    public int getStatus() throws SystemException {
      return 0;
    }

    @Override
    public Transaction getTransaction() throws SystemException {
      return currentTransaction;
    }

    @Override
    public void resume(Transaction tobj) throws InvalidTransactionException, IllegalStateException, SystemException {

    }

    @Override
    public void rollback() throws IllegalStateException, SecurityException, SystemException {
      currentTransaction.rollback();
      currentTransaction = null;
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException {

    }

    @Override
    public void setTransactionTimeout(int seconds) throws SystemException {

    }

    @Override
    public Transaction suspend() throws SystemException {
      return null;
    }
  }

  static class TestTransaction implements Transaction {

    final long gtrid;
    final Map<XAResource, TestXid> xids = new IdentityHashMap<XAResource, TestXid>();
    final AtomicLong bqualGenerator = new AtomicLong();
    final List<Synchronization> synchronizations = new CopyOnWriteArrayList<Synchronization>();

    public TestTransaction(long gtrid) {
      this.gtrid = gtrid;
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
      try {
        Set<Map.Entry<XAResource, TestXid>> entries = xids.entrySet();

        // delist
        for (Map.Entry<XAResource, TestXid> entry : entries) {
          try {
            entry.getKey().end(entry.getValue(), XAResource.TMSUCCESS);
          } catch (XAException e) {
            throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
          }
        }

        fireBeforeCompletion();

        Set<XAResource> preparedResources = new HashSet<XAResource>();

        // prepare
        for (Map.Entry<XAResource, TestXid> entry : entries) {
          try {
            int prepareStatus = entry.getKey().prepare(entry.getValue());
            if (prepareStatus != XAResource.XA_RDONLY) {
              preparedResources.add(entry.getKey());
            }
          } catch (XAException e) {
            throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
          }
        }

        // commit
        for (Map.Entry<XAResource, TestXid> entry : entries) {
          try {
            if (preparedResources.contains(entry.getKey())) {
              entry.getKey().commit(entry.getValue(), false);
            }
          } catch (XAException e) {
            throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
          }
        }
      } finally {
        fireAfterCompletion(Status.STATUS_COMMITTED);
      }
    }

    @Override
    public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
      return true;
    }

    @Override
    public boolean enlistResource(XAResource xaRes) throws RollbackException, IllegalStateException, SystemException {
      TestXid testXid = xids.get(xaRes);
      if (testXid == null) {
        testXid = new TestXid(gtrid, bqualGenerator.incrementAndGet());
        xids.put(xaRes, testXid);
      }

      try {
        xaRes.start(testXid, XAResource.TMNOFLAGS);
      } catch (XAException e) {
        throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
      }
      return true;
    }

    @Override
    public int getStatus() throws SystemException {
      return 0;
    }

    @Override
    public void registerSynchronization(Synchronization sync) throws RollbackException, IllegalStateException, SystemException {
      synchronizations.add(sync);
    }

    @Override
    public void rollback() throws IllegalStateException, SystemException {
      try {
        Set<Map.Entry<XAResource, TestXid>> entries = xids.entrySet();

        // delist
        for (Map.Entry<XAResource, TestXid> entry : entries) {
          try {
            entry.getKey().end(entry.getValue(), XAResource.TMSUCCESS);
          } catch (XAException e) {
            throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
          }
        }

        // rollback
        for (Map.Entry<XAResource, TestXid> entry : entries) {
          try {
            entry.getKey().rollback(entry.getValue());
          } catch (XAException e) {
            throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
          }
        }
      } finally {
        fireAfterCompletion(Status.STATUS_ROLLEDBACK);
      }
    }

    private void fireBeforeCompletion() {
      for (Synchronization synchronization : synchronizations) {
        synchronization.beforeCompletion();
      }
    }

    private void fireAfterCompletion(int status) {
      for (Synchronization synchronization : synchronizations) {
        synchronization.afterCompletion(status);
      }
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException {

    }
  }


}
