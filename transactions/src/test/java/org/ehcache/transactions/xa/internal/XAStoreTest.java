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

package org.ehcache.transactions.xa.internal;

import org.ehcache.Cache;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.service.DiskResourceService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.impl.internal.spi.copy.DefaultCopyProvider;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.offheap.MemorySizeParser;
import org.ehcache.impl.internal.store.offheap.OffHeapStore;
import org.ehcache.impl.internal.store.offheap.OffHeapStoreLifecycleHelper;
import org.ehcache.impl.internal.store.tiering.TieredStore;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.transactions.xa.XACacheException;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.ehcache.transactions.xa.internal.journal.Journal;
import org.ehcache.transactions.xa.internal.journal.TransientJournal;
import org.ehcache.transactions.xa.internal.txmgr.NullXAResourceRegistry;
import org.ehcache.transactions.xa.txmgr.TransactionManagerWrapper;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerProvider;
import org.ehcache.transactions.xa.utils.JavaSerializer;
import org.ehcache.transactions.xa.utils.TestXid;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptySet;
import static org.ehcache.config.builders.ExpiryPolicyBuilder.timeToLiveExpiration;
import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link XAStore} and {@link org.ehcache.transactions.xa.internal.XAStore.Provider}.
 */
public class XAStoreTest {

  private static final Supplier<Boolean> SUPPLY_TRUE = () -> true;
  private static final Supplier<Boolean> SUPPLY_FALSE = () -> false;

  @Rule
  public TestName testName = new TestName();

  @SuppressWarnings("unchecked")
  private final Class<SoftLock<String>> valueClass = (Class) SoftLock.class;
  private final TestTransactionManager testTransactionManager = new TestTransactionManager();
  private TransactionManagerWrapper transactionManagerWrapper;
  private OnHeapStore<Long, SoftLock<String>> onHeapStore;
  private Journal<Long> journal;
  private TestTimeSource testTimeSource;
  private ClassLoader classLoader;
  private Serializer<Long> keySerializer;
  private Serializer<SoftLock<String>> valueSerializer;
  private StoreEventDispatcher<Long, SoftLock<String>> eventDispatcher;
  private final ExpiryPolicy<Object, Object> expiry = timeToLiveExpiration(ofSeconds(1));
  private Copier<Long> keyCopier;
  private Copier<SoftLock<String>> valueCopier;

  @Before
  public void setUp() {
    transactionManagerWrapper = new TransactionManagerWrapper(testTransactionManager, new NullXAResourceRegistry());
    classLoader = ClassLoader.getSystemClassLoader();
    keySerializer = new JavaSerializer<>(classLoader);
    valueSerializer = new JavaSerializer<>(classLoader);
    CopyProvider copyProvider = new DefaultCopyProvider(new DefaultCopyProviderConfiguration());
    keyCopier = copyProvider.createKeyCopier(Long.class, keySerializer);
    valueCopier = copyProvider.createValueCopier(valueClass, valueSerializer);
    Store.Configuration<Long, SoftLock<String>> onHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass,
      null, classLoader, ExpiryPolicyBuilder.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder()
      .heap(10, EntryUnit.ENTRIES)
      .build(),
      0, keySerializer, valueSerializer);
    testTimeSource = new TestTimeSource();
    eventDispatcher = NullStoreEventDispatcher.nullStoreEventDispatcher();
    onHeapStore = new OnHeapStore<>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    journal = new TransientJournal<>();
  }

  @Test
  public void testXAStoreProviderFailsToRankWhenNoTMProviderConfigured() throws Exception {
    XAStore.Provider provider = new XAStore.Provider();
      provider.start(new ServiceProvider<Service>() {
        @Override
        public <U extends Service> U getService(Class<U> serviceType) {
          return null;
        }

        @Override
        public <U extends Service> Collection<U> getServicesOfType(Class<U> serviceType) {
          return emptySet();
        }
      });
    try {
      provider.wrapperStoreRank(Collections.singleton(mock(XAStoreConfiguration.class)));
      fail("Expected exception");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("TransactionManagerProvider"));
    }
  }

  @Test
  public void testSimpleGetPutRemove() throws Exception {
    XAStore<Long, String> xaStore = getXAStore(onHeapStore);

    testTransactionManager.begin();
    {
      assertThat(xaStore.remove(1L), equalTo(false));
      assertThat(xaStore.get(1L), is(nullValue()));
      assertThat(xaStore.put(1L, "1"), equalTo(Store.PutStatus.PUT));
      assertThat(xaStore.put(1L, "one"), equalTo(Store.PutStatus.PUT));
      assertThat(xaStore.get(1L).get(), equalTo("one"));
    }
    testTransactionManager.rollback();

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      assertThat(xaStore.get(1L), is(nullValue()));
      assertThat(xaStore.put(1L, "1"), equalTo(Store.PutStatus.PUT));
      assertThat(xaStore.put(1L, "one"), equalTo(Store.PutStatus.PUT));
      assertThat(xaStore.get(1L).get(), equalTo("one"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      assertThat(xaStore.remove(1L), equalTo(true));
      assertThat(xaStore.remove(1L), equalTo(false));
      assertThat(xaStore.get(1L), is(nullValue()));
      assertThat(xaStore.put(1L, "1"), equalTo(Store.PutStatus.PUT));
    }
    testTransactionManager.rollback();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      assertThat(xaStore.put(1L, "un"), equalTo(Store.PutStatus.PUT));
      assertThat(xaStore.remove(1L), equalTo(true));
      assertThat(xaStore.remove(1L), equalTo(false));
      assertThat(xaStore.get(1L), is(nullValue()));
      assertThat(xaStore.put(1L, "un"), equalTo(Store.PutStatus.PUT));
      assertThat(xaStore.get(1L).get(), equalTo("un"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "un");
  }

  @Test
  public void testConflictingGetPutRemove() throws Exception {
    final XAStore<Long, String> xaStore = getXAStore(onHeapStore);
    final AtomicReference<Throwable> exception = new AtomicReference<>();

    testTransactionManager.begin();
    {
      xaStore.put(1L, "one");
    }
    testTransactionManager.commit();

    testTransactionManager.begin();
    {
      assertThat(xaStore.put(1L, "un"), equalTo(Store.PutStatus.PUT));

      executeWhileIn2PC(exception, () -> {
        testTransactionManager.begin();
        assertThat(xaStore.put(1L, "uno"), equalTo(Store.PutStatus.NOOP));
        testTransactionManager.commit();
        return null;
      });

      assertThat(xaStore.put(1L, "eins"), equalTo(Store.PutStatus.PUT));
    }
    testTransactionManager.commit();
    assertThat(exception.get(), is(nullValue()));

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      xaStore.put(1L, "one");
    }
    testTransactionManager.commit();

    testTransactionManager.begin();
    {
      assertThat(xaStore.put(1L, "un"), equalTo(Store.PutStatus.PUT));

      executeWhileIn2PC(exception, () -> {
        testTransactionManager.begin();

        assertThat(xaStore.remove(1L), is(false));

        testTransactionManager.commit();
        return null;
      });

      assertThat(xaStore.put(1L, "een"), equalTo(Store.PutStatus.PUT));
    }
    testTransactionManager.commit();

    assertThat(exception.get(), is(nullValue()));
    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      xaStore.put(1L, "one");
    }
    testTransactionManager.commit();

    testTransactionManager.begin();
    {
      assertThat(xaStore.put(1L, "un"), equalTo(Store.PutStatus.PUT));

      executeWhileIn2PC(exception, () -> {
        testTransactionManager.begin();

        assertThat(xaStore.get(1L), is(nullValue()));

        testTransactionManager.commit();
        return null;
      });

      assertThat(xaStore.put(1L, "yksi"), equalTo(Store.PutStatus.PUT));
    }
    testTransactionManager.commit();

    assertThat(exception.get(), is(nullValue()));
    assertMapping(xaStore, 1L, null);
  }

  private void executeWhileIn2PC(AtomicReference<Throwable> exception, Callable<?> callable) {
    testTransactionManager.getCurrentTransaction().registerTwoPcListener(() -> {
      try {
        Thread t = new Thread(() -> {
          try {
            // this runs while the committing TX is in-doubt
            callable.call();
          } catch (Throwable t1) {
            exception.set(t1);
          }
        });
        t.start();
        t.join();
      } catch (Throwable e) {
        exception.set(e);
      }
    });
  }

  @Test
  public void testIterate() throws Exception {
    XAStore<Long, String> xaStore = getXAStore(onHeapStore);

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

      Map<Long, String> iterated = new HashMap<>();
      Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator = xaStore.iterator();
      while (iterator.hasNext()) {
        Cache.Entry<Long, Store.ValueHolder<String>> next = iterator.next();
        iterated.put(next.getKey(), next.getValue().get());
      }
      assertThat(iterated.size(), is(3));
      assertThat(iterated.get(0L), equalTo("zero"));
      assertThat(iterated.get(1L), equalTo("un"));
      assertThat(iterated.get(2L), equalTo("two"));
    }
    testTransactionManager.commit();

    testTransactionManager.begin();
    {
      Map<Long, String> iterated = new HashMap<>();
      Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator = xaStore.iterator();
      while (iterator.hasNext()) {
        Cache.Entry<Long, Store.ValueHolder<String>> next = iterator.next();
        iterated.put(next.getKey(), next.getValue().get());
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
  public void testPutIfAbsent() throws Exception {
    final XAStore<Long, String> xaStore = getXAStore(onHeapStore);
    final AtomicReference<Throwable> exception = new AtomicReference<>();

    testTransactionManager.begin();
    {
      assertThat(xaStore.putIfAbsent(1L, "one", b -> {}), is(nullValue()));
      assertThat(xaStore.get(1L).get(), equalTo("one"));
      assertThat(xaStore.putIfAbsent(1L, "un", b -> {}).get(), equalTo("one"));
      assertThat(xaStore.get(1L).get(), equalTo("one"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      assertThat(xaStore.putIfAbsent(1L, "un", b -> {}).get(), equalTo("one"));
      assertThat(xaStore.get(1L).get(), equalTo("one"));
      assertThat(xaStore.remove(1L), equalTo(true));
      assertThat(xaStore.putIfAbsent(1L, "uno", b -> {}), is(nullValue()));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "uno");

    testTransactionManager.begin();
    {
      xaStore.put(1L, "eins");
      executeWhileIn2PC(exception, () -> {
        testTransactionManager.begin();

        assertThat(xaStore.putIfAbsent(1L, "un", b -> {}), is(nullValue()));

        testTransactionManager.commit();
        return null;
      });
    }
    testTransactionManager.commit();
    assertThat(exception.get(), is(nullValue()));

    assertMapping(xaStore, 1L, null);
  }

  @Test
  public void testRemove2Args() throws Exception {
    final XAStore<Long, String> xaStore = getXAStore(onHeapStore);
    final AtomicReference<Throwable> exception = new AtomicReference<>();

    testTransactionManager.begin();
    {
      assertThat(xaStore.remove(1L, "one"), equalTo(Store.RemoveStatus.KEY_MISSING));
      assertThat(xaStore.put(1L, "one"), equalTo(Store.PutStatus.PUT));
      assertThat(xaStore.remove(1L, "un"), equalTo(Store.RemoveStatus.KEY_PRESENT));
      assertThat(xaStore.remove(1L, "one"), equalTo(Store.RemoveStatus.REMOVED));
      assertThat(xaStore.remove(1L, "eins"), equalTo(Store.RemoveStatus.KEY_MISSING));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      assertThat(xaStore.put(1L, "one"), equalTo(Store.PutStatus.PUT));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      assertThat(xaStore.remove(1L, "een"), equalTo(Store.RemoveStatus.KEY_PRESENT));
      assertThat(xaStore.remove(1L, "one"), equalTo(Store.RemoveStatus.REMOVED));
      assertThat(xaStore.remove(1L, "eins"), equalTo(Store.RemoveStatus.KEY_MISSING));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      xaStore.put(1L, "eins");
      executeWhileIn2PC(exception, () -> {
        testTransactionManager.begin();

        assertThat(xaStore.remove(1L, "un"), equalTo(Store.RemoveStatus.KEY_MISSING));

        testTransactionManager.commit();
        return null;
      });
    }
    testTransactionManager.commit();
    assertThat(exception.get(), is(nullValue()));

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      assertThat(xaStore.put(1L, "one"), equalTo(Store.PutStatus.PUT));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      xaStore.put(1L, "eins");
      executeWhileIn2PC(exception, () -> {
        testTransactionManager.begin();

        assertThat(xaStore.remove(1L, "un"), equalTo(Store.RemoveStatus.KEY_MISSING));

        testTransactionManager.commit();
        return null;
      });
    }
    testTransactionManager.commit();
    assertThat(exception.get(), is(nullValue()));

    assertMapping(xaStore, 1L, null);
  }

  @Test
  public void testReplace2Args() throws Exception {
    final XAStore<Long, String> xaStore = getXAStore(onHeapStore);
    final AtomicReference<Throwable> exception = new AtomicReference<>();

    testTransactionManager.begin();
    {
      assertThat(xaStore.replace(1L, "one"), is(nullValue()));
      assertThat(xaStore.put(1L, "one"), equalTo(Store.PutStatus.PUT));
      assertThat(xaStore.replace(1L, "un").get(), equalTo("one"));
      assertThat(xaStore.replace(1L, "uno").get(), equalTo("un"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "uno");

    testTransactionManager.begin();
    {
      assertThat(xaStore.replace(1L, "een").get(), equalTo("uno"));
      assertThat(xaStore.replace(1L, "eins").get(), equalTo("een"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "eins");

    testTransactionManager.begin();
    {
      assertThat(xaStore.remove(1L), is(true));
      assertThat(xaStore.replace(1L, "yksi"), is(nullValue()));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      xaStore.put(1L, "eins");
      executeWhileIn2PC(exception, () -> {
        testTransactionManager.begin();

        assertThat(xaStore.replace(1L, "un"), is(nullValue()));

        testTransactionManager.commit();
        return null;
      });
    }
    testTransactionManager.commit();
    assertThat(exception.get(), is(nullValue()));

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      assertThat(xaStore.put(1L, "one"), is(Store.PutStatus.PUT));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      xaStore.put(1L, "eins");
      executeWhileIn2PC(exception, () -> {
        testTransactionManager.begin();

        assertThat(xaStore.replace(1L, "un"), is(nullValue()));

        testTransactionManager.commit();
        return null;
      });
    }
    testTransactionManager.commit();
    assertThat(exception.get(), is(nullValue()));

    assertMapping(xaStore, 1L, null);
  }

  @Test
  public void testReplace3Args() throws Exception {
    final XAStore<Long, String> xaStore = getXAStore(onHeapStore);
    final AtomicReference<Throwable> exception = new AtomicReference<>();

    testTransactionManager.begin();
    {
      assertThat(xaStore.replace(1L, "one", "un"), equalTo(Store.ReplaceStatus.MISS_NOT_PRESENT));
      assertThat(xaStore.put(1L, "one"), equalTo(Store.PutStatus.PUT));
      assertThat(xaStore.replace(1L, "eins", "un"), equalTo(Store.ReplaceStatus.MISS_PRESENT));
      assertThat(xaStore.replace(1L, "one", "un"), equalTo(Store.ReplaceStatus.HIT));
      assertThat(xaStore.get(1L).get(), equalTo("un"));
      assertThat(xaStore.replace(1L, "eins", "een"), equalTo(Store.ReplaceStatus.MISS_PRESENT));
      assertThat(xaStore.replace(1L, "un", "uno"), equalTo(Store.ReplaceStatus.HIT));
      assertThat(xaStore.get(1L).get(), equalTo("uno"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "uno");

    testTransactionManager.begin();
    {
      assertThat(xaStore.replace(1L, "one", "uno"), equalTo(Store.ReplaceStatus.MISS_PRESENT));
      assertThat(xaStore.replace(1L, "uno", "un"), equalTo(Store.ReplaceStatus.HIT));
      assertThat(xaStore.get(1L).get(), equalTo("un"));
      assertThat(xaStore.remove(1L), equalTo(true));
      assertThat(xaStore.replace(1L, "un", "eins"), equalTo(Store.ReplaceStatus.MISS_NOT_PRESENT));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      xaStore.put(1L, "eins");
      executeWhileIn2PC(exception, () -> {
        testTransactionManager.begin();

        assertThat(xaStore.replace(1L, "eins", "one"), is(Store.ReplaceStatus.MISS_NOT_PRESENT));

        testTransactionManager.commit();
        return null;
      });
    }
    testTransactionManager.commit();
    assertThat(exception.get(), is(nullValue()));

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      assertThat(xaStore.put(1L, "one"), is(Store.PutStatus.PUT));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      xaStore.put(1L, "eins");
      executeWhileIn2PC(exception, () -> {
        testTransactionManager.begin();

        assertThat(xaStore.replace(1L, "one", "un"), is(Store.ReplaceStatus.MISS_NOT_PRESENT));

        testTransactionManager.commit();
        return null;
      });
    }
    testTransactionManager.commit();
    assertThat(exception.get(), is(nullValue()));

    assertMapping(xaStore, 1L, null);
  }

  @Test
  public void testGetAndCompute() throws Exception {
    Store.Configuration<Long, SoftLock<String>> offHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass,
            null, classLoader, ExpiryPolicyBuilder.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder()
            .offheap(10, MemoryUnit.MB)
            .build(),
            0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = new OffHeapStore<>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser
            .parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    TieredStore<Long, SoftLock<String>> tieredStore = new TieredStore<>(onHeapStore, offHeapStore);

    XAStore<Long, String> xaStore = getXAStore(tieredStore);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, is(nullValue()));
        return "one";
      });
      assertThat(computed1, is(nullValue()));
      assertThat(xaStore.get(1L).get(), equalTo("one"));
      Store.ValueHolder<String> computed2 = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("one"));
        return "un";
      });
      assertThat(computed2.get(), equalTo("one"));
      assertThat(xaStore.get(1L).get(), equalTo("un"));
      Store.ValueHolder<String> computed3 = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("un"));
        return null;
      });
      assertThat(computed3.get(), equalTo("un"));
      assertThat(xaStore.get(1L), is(nullValue()));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, is(nullValue()));
        return "one";
      });
      assertThat(computed1, is(nullValue()));
      assertThat(xaStore.get(1L).get(), equalTo("one"));
      Store.ValueHolder<String> computed2 = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("one"));
        return null;
      });
      assertThat(computed2.get(), equalTo("one"));
      assertThat(xaStore.get(1L), is(nullValue()));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, is(nullValue()));
        return "one";
      });
      assertThat(computed1, is(nullValue()));
      assertThat(xaStore.get(1L).get(), equalTo("one"));
      Store.ValueHolder<String> computed2 = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("one"));
        return null;
      });
      assertThat(computed2.get(), equalTo("one"));
      assertThat(xaStore.get(1L), is(nullValue()));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, is(nullValue()));
        return "one";
      });
      assertThat(computed1, is(nullValue()));
      assertThat(xaStore.get(1L).get(), equalTo("one"));
      Store.ValueHolder<String> computed2 = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("one"));
        return "un";
      });
      assertThat(computed2.get(), equalTo("one"));
      assertThat(xaStore.get(1L).get(), equalTo("un"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "un");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("un"));
        return "eins";
      });
      assertThat(computed.get(), equalTo("un"));
      assertThat(xaStore.get(1L).get(), equalTo("eins"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "eins");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("eins"));
        return null;
      });
      assertThat(computed.get(), equalTo("eins"));
      assertThat(xaStore.get(1L), is(nullValue()));
    }
    testTransactionManager.rollback();

    assertMapping(xaStore, 1L, "eins");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("eins"));
        return null;
      });
      assertThat(computed1.get(), equalTo("eins"));
      assertThat(xaStore.get(1L), is(nullValue()));
      Store.ValueHolder<String> computed2 = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, is(nullValue()));
        return null;
      });
      assertThat(computed2, is(nullValue()));
      assertThat(xaStore.get(1L), is(nullValue()));
      Store.ValueHolder<String> computed3 = xaStore.getAndCompute(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, is(nullValue()));
        return "uno";
      });
      assertThat(computed3, is(nullValue()));
      assertThat(xaStore.get(1L).get(), equalTo("uno"));
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
  public void testCompute() throws Exception {
    Store.Configuration<Long, SoftLock<String>> offHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass,
      null, classLoader, ExpiryPolicyBuilder.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder()
      .offheap(10, MemoryUnit.MB)
      .build(),
      0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = new OffHeapStore<>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser
      .parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    TieredStore<Long, SoftLock<String>> tieredStore = new TieredStore<>(onHeapStore, offHeapStore);

    XAStore<Long, String> xaStore = getXAStore(tieredStore);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, is(nullValue()));
        return "one";
      }, SUPPLY_TRUE, SUPPLY_FALSE);
      assertThat(computed1.get(), equalTo("one"));
      Store.ValueHolder<String> computed2 = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("one"));
        return "un";
      }, SUPPLY_TRUE, SUPPLY_FALSE);
      assertThat(computed2.get(), equalTo("un"));
      Store.ValueHolder<String> computed3 = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("un"));
        return null;
      }, SUPPLY_TRUE, SUPPLY_FALSE);
      assertThat(computed3, is(nullValue()));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, is(nullValue()));
        return "one";
      }, SUPPLY_FALSE, SUPPLY_FALSE);
      assertThat(computed1.get(), equalTo("one"));
      Store.ValueHolder<String> computed2 = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("one"));
        return null;
      }, SUPPLY_FALSE, SUPPLY_FALSE);
      assertThat(computed2, is(nullValue()));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, is(nullValue()));
        return "one";
      }, SUPPLY_TRUE, SUPPLY_FALSE);
      assertThat(computed1.get(), equalTo("one"));
      Store.ValueHolder<String> computed2 = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("one"));
        return null;
      }, SUPPLY_TRUE, SUPPLY_FALSE);
      assertThat(computed2, is(nullValue()));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, is(nullValue()));
        return "one";
      }, SUPPLY_TRUE, SUPPLY_FALSE);
      assertThat(computed1.get(), equalTo("one"));
      Store.ValueHolder<String> computed2 = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("one"));
        return "un";
      }, SUPPLY_TRUE, SUPPLY_FALSE);
      assertThat(computed2.get(), equalTo("un"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "un");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("un"));
        return "eins";
      }, SUPPLY_TRUE, SUPPLY_FALSE);
      assertThat(computed.get(), equalTo("eins"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "eins");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("eins"));
        return null;
      }, SUPPLY_TRUE, SUPPLY_FALSE);
      assertThat(computed, is(nullValue()));
    }
    testTransactionManager.rollback();

    assertMapping(xaStore, 1L, "eins");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, equalTo("eins"));
        return null;
      }, SUPPLY_TRUE, SUPPLY_FALSE);
      assertThat(computed1, is(nullValue()));
      Store.ValueHolder<String> computed2 = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, is(nullValue()));
        return null;
      }, SUPPLY_TRUE, SUPPLY_FALSE);
      assertThat(computed2, is(nullValue()));
      Store.ValueHolder<String> computed3 = xaStore.computeAndGet(1L, (aLong, s) -> {
        assertThat(aLong, is(1L));
        assertThat(s, is(nullValue()));
        return "uno";
      }, SUPPLY_TRUE, SUPPLY_FALSE);
      assertThat(computed3.get(), equalTo("uno"));
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
    Store.Configuration<Long, SoftLock<String>> offHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass, null,
      classLoader, ExpiryPolicyBuilder.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder()
      .offheap(10, MemoryUnit.MB)
      .build(),
      0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = new OffHeapStore<>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser
      .parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    TieredStore<Long, SoftLock<String>> tieredStore = new TieredStore<>(onHeapStore, offHeapStore);

    XAStore<Long, String> xaStore = getXAStore(tieredStore);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.computeIfAbsent(1L, aLong -> {
        assertThat(aLong, is(1L));
        return "one";
      });
      assertThat(computed1.get(), equalTo("one"));
      Store.ValueHolder<String> computed2 = xaStore.computeIfAbsent(1L, aLong -> {
        fail("should not be absent");
        throw new AssertionError();
      });
      assertThat(computed2.get(), equalTo("one"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.computeIfAbsent(1L, aLong -> {
        fail("should not be absent");
        throw new AssertionError();
      });
      assertThat(computed1.get(), equalTo("one"));

      xaStore.remove(1L);

      Store.ValueHolder<String> computed2 = xaStore.computeIfAbsent(1L, aLong -> {
        assertThat(aLong, is(1L));
        return "un";
      });
      assertThat(computed2.get(), equalTo("un"));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "un");

    OffHeapStoreLifecycleHelper.close(offHeapStore);
  }

  @Test
  public void testExpiry() throws Exception {
    Store.Configuration<Long, SoftLock<String>> onHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass,
      null, classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
      0, keySerializer, valueSerializer);
    onHeapStore = new OnHeapStore<>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock<String>> offHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass, null,
      classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
      0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = new OffHeapStore<>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser
      .parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    TieredStore<Long, SoftLock<String>> tieredStore = new TieredStore<>(onHeapStore, offHeapStore);

    XAStore<Long, String> xaStore = getXAStore(tieredStore);

    testTransactionManager.begin();
    {
      xaStore.put(1L, "one");
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "one");

    testTimeSource.advanceTime(2000);

    assertMapping(xaStore, 1L, null);

    OffHeapStoreLifecycleHelper.close(offHeapStore);
  }

  @Test
  public void testExpiryCreateException() throws Exception {
    ExpiryPolicy<Object, Object> expiry = new ExpiryPolicy<Object, Object>() {

      @Override
      public Duration getExpiryForCreation(Object key, Object value) {
        throw new RuntimeException();
      }

      @Override
      public Duration getExpiryForAccess(Object key, Supplier<?> value) {
        throw new AssertionError();
      }

      @Override
      public Duration getExpiryForUpdate(Object key, Supplier<?> oldValue, Object newValue) {
        throw new AssertionError();
      }
    };
    Store.Configuration<Long, SoftLock<String>> onHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass, null,
      classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
      0, keySerializer, valueSerializer);
    OnHeapStore<Long, SoftLock<String>> onHeapStore = new OnHeapStore<>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock<String>> offHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass, null,
      classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
      0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = new OffHeapStore<>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser
      .parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    TieredStore<Long, SoftLock<String>> tieredStore = new TieredStore<>(onHeapStore, offHeapStore);

    XAStore<Long, String> xaStore = getXAStore(tieredStore);

    testTransactionManager.begin();
    xaStore.put(1L, "one");
    testTransactionManager.commit();
    assertMapping(xaStore, 1L, null);
  }

  @Test
  public void testExpiryAccessException() throws Exception {
    String uniqueXAResourceId = "testExpiryAccessException";
    ExpiryPolicy<Object, Object> expiry = new ExpiryPolicy<Object, Object>() {

      @Override
      public Duration getExpiryForCreation(Object key, Object value) {
        return ExpiryPolicy.INFINITE;
      }

      @Override
      public Duration getExpiryForAccess(Object key, Supplier<?> value) {
        if (testTimeSource.getTimeMillis() > 0) {
          throw new RuntimeException();
        }
        return ExpiryPolicy.INFINITE;
      }

      @Override
      public Duration getExpiryForUpdate(Object key, Supplier<?> oldValue, Object newValue) {
        return ExpiryPolicy.INFINITE;
      }
    };
    Store.Configuration<Long, SoftLock<String>> onHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass, null,
      classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
      0, keySerializer, valueSerializer);
    OnHeapStore<Long, SoftLock<String>> onHeapStore = new OnHeapStore<>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock<String>> offHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass, null,
      classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
      0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = new OffHeapStore<>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser
      .parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    TieredStore<Long, SoftLock<String>> tieredStore = new TieredStore<>(onHeapStore, offHeapStore);

    XAStore<Long, String> xaStore = getXAStore(tieredStore);

    testTransactionManager.begin();
    xaStore.put(1L, "one");
    testTransactionManager.commit();

    testTimeSource.advanceTime(1000);
    testTransactionManager.begin();
    assertThat(xaStore.get(1L).get(), is("one"));
    testTransactionManager.commit();

    testTransactionManager.begin();
    assertThat(xaStore.get(1L), nullValue());
    testTransactionManager.commit();
  }

  @Test
  public void testExpiryUpdateException() throws Exception{
    ExpiryPolicy<Object, Object> expiry = new ExpiryPolicy<Object, Object>() {

      @Override
      public Duration getExpiryForCreation(Object key, Object value) {
        return ExpiryPolicy.INFINITE;
      }

      @Override
      public Duration getExpiryForAccess(Object key, Supplier<?> value) {
        return ExpiryPolicy.INFINITE;
      }

      @Override
      public Duration getExpiryForUpdate(Object key, Supplier<?> oldValue, Object newValue) {
        if (testTimeSource.getTimeMillis() > 0) {
          throw new RuntimeException();
        }
        return ExpiryPolicy.INFINITE;
      }
    };
    Store.Configuration<Long, SoftLock<String>> onHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass, null,
      classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
      0, keySerializer, valueSerializer);
    OnHeapStore<Long, SoftLock<String>> onHeapStore = new OnHeapStore<>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock<String>> offHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass, null,
      classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
      0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = new OffHeapStore<>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser
      .parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    TieredStore<Long, SoftLock<String>> tieredStore = new TieredStore<>(onHeapStore, offHeapStore);

    XAStore<Long, String> xaStore = getXAStore(tieredStore);

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
    ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(1));
    Store.Configuration<Long, SoftLock<String>> onHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass, null,
      classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
      0, keySerializer, valueSerializer);
    OnHeapStore<Long, SoftLock<String>> onHeapStore = new OnHeapStore<>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock<String>> offHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass, null,
      classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
      0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = new OffHeapStore<>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser
      .parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    TieredStore<Long, SoftLock<String>> tieredStore = new TieredStore<>(onHeapStore, offHeapStore);

    XAStore<Long, String> xaStore = getXAStore(tieredStore);

    testTransactionManager.begin();
    {
      Map<Long, Store.ValueHolder<String>> computedMap = xaStore.bulkCompute(asSet(1L, 2L, 3L), entries -> {
        Map<Long, String> result = new HashMap<>();
        for (Map.Entry<? extends Long, ? extends String> entry : entries) {
          Long key = entry.getKey();
          String value = entry.getValue();
          assertThat(value, is(nullValue()));
          result.put(key, "stuff#" + key);
        }
        return result.entrySet();
      });

      assertThat(computedMap.size(), is(3));
      assertThat(computedMap.get(1L).get(), equalTo("stuff#1"));
      assertThat(computedMap.get(2L).get(), equalTo("stuff#2"));
      assertThat(computedMap.get(3L).get(), equalTo("stuff#3"));

      computedMap = xaStore.bulkCompute(asSet(0L, 1L, 3L), entries -> {
        Map<Long, String> result = new HashMap<>();
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
      });

      assertThat(computedMap.size(), is(3));
      assertThat(computedMap.get(0L).get(), equalTo("otherStuff#0"));
      assertThat(computedMap.get(1L).get(), equalTo("otherStuff#1"));
      assertThat(computedMap.get(3L), is(nullValue()));
    }
    testTransactionManager.commit();

    assertSize(xaStore, 3);
    assertMapping(xaStore, 0L, "otherStuff#0");
    assertMapping(xaStore, 1L, "otherStuff#1");
    assertMapping(xaStore, 2L, "stuff#2");

    OffHeapStoreLifecycleHelper.close(offHeapStore);
  }

  @Test
  public void testBulkComputeIfAbsent() throws Exception {
    ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(1));
    Store.Configuration<Long, SoftLock<String>> onHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass, null,
      classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(),
      0, keySerializer, valueSerializer);
    OnHeapStore<Long, SoftLock<String>> onHeapStore = new OnHeapStore<>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);
    Store.Configuration<Long, SoftLock<String>> offHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass, null,
      classLoader, expiry, ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(),
      0, keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = new OffHeapStore<>(offHeapConfig, testTimeSource, eventDispatcher, MemorySizeParser
      .parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    TieredStore<Long, SoftLock<String>> tieredStore = new TieredStore<>(onHeapStore, offHeapStore);

    XAStore<Long, String> xaStore = getXAStore(tieredStore);

    testTransactionManager.begin();
    {
      Map<Long, Store.ValueHolder<String>> computedMap = xaStore.bulkComputeIfAbsent(asSet(1L, 2L, 3L), keys -> {
        Map<Long, String> result = new HashMap<>();
        for (Long key : keys) {
          result.put(key, "stuff#" + key);
        }
        return result.entrySet();
      });

      assertThat(computedMap.size(), is(3));
      assertThat(computedMap.get(1L).get(), equalTo("stuff#1"));
      assertThat(computedMap.get(2L).get(), equalTo("stuff#2"));
      assertThat(computedMap.get(3L).get(), equalTo("stuff#3"));

      computedMap = xaStore.bulkComputeIfAbsent(asSet(0L, 1L, 3L), keys -> {
        Map<Long, String> result = new HashMap<>();
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
      });

      assertThat(computedMap.size(), is(3));
      assertThat(computedMap.get(0L).get(), equalTo("otherStuff#0"));
      assertThat(computedMap.get(1L).get(), equalTo("stuff#1"));
      assertThat(computedMap.get(3L).get(), equalTo("stuff#3"));
    }
    testTransactionManager.commit();

    assertSize(xaStore, 4);
    assertMapping(xaStore, 0L, "otherStuff#0");
    assertMapping(xaStore, 1L, "stuff#1");
    assertMapping(xaStore, 2L, "stuff#2");
    assertMapping(xaStore, 3L, "stuff#3");

    OffHeapStoreLifecycleHelper.close(offHeapStore);
  }

  @Test
  public void testCustomEvictionAdvisor() throws Exception {
    final AtomicBoolean invoked = new AtomicBoolean();

    EvictionAdvisor<Long, SoftLock<String>> evictionAdvisor = (key, value) -> {
      invoked.set(true);
      return false;
    };
    Store.Configuration<Long, SoftLock<String>> onHeapConfig = new StoreConfigurationImpl<>(Long.class, valueClass,
      evictionAdvisor, classLoader, ExpiryPolicyBuilder.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder()
      .heap(10, EntryUnit.ENTRIES)
      .build(),
      0, keySerializer, valueSerializer);
    OnHeapStore<Long, SoftLock<String>> onHeapStore = new OnHeapStore<>(onHeapConfig, testTimeSource, keyCopier, valueCopier, new NoopSizeOfEngine(), eventDispatcher);

    final XAStore<Long, String> xaStore = getXAStore(onHeapStore);

    testTransactionManager.begin();
    {
      xaStore.put(1L, "1");
    }
    testTransactionManager.rollback();
    assertThat(invoked.get(), is(false));

    testTransactionManager.begin();
    {
      xaStore.put(1L, "1");
    }
    testTransactionManager.commit();
    assertThat(invoked.get(), is(true));
  }

  @Test
  public void testRank() throws Exception {
    XAStore.Provider provider = new XAStore.Provider();
    XAStoreConfiguration configuration = new XAStoreConfiguration("testXAResourceId");
    ServiceLocator serviceLocator = dependencySet()
      .with(provider)
      .with(Store.Provider.class)
      .with(mock(DiskResourceService.class))
      .with(mock(TransactionManagerProvider.class)).build();

    serviceLocator.startAllServices();

    Set<ServiceConfiguration<?>> xaStoreConfigs = Collections.singleton(configuration);
    assertThat(provider.wrapperStoreRank(xaStoreConfigs), is(1));

    Set<ServiceConfiguration<?>> emptyConfigs = emptySet();
    assertThat(provider.wrapperStoreRank(emptyConfigs), is(0));

  }

  private Set<Long> asSet(Long... longs) {
    return new HashSet<>(Arrays.asList(longs));
  }

  private void assertMapping(XAStore<Long, String> xaStore, long key, String value) throws Exception {
    testTransactionManager.begin();

    Store.ValueHolder<String> valueHolder = xaStore.get(key);
    if (value != null) {
      assertThat(valueHolder.get(), equalTo(value));
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
      iterator.next();
      counter++;
    }
    assertThat(counter, is(expectedSize));

    testTransactionManager.commit();
  }

  private XAStore<Long, String> getXAStore(Store<Long, SoftLock<String>> store) {
    return new XAStore<>(Long.class, String.class, store, transactionManagerWrapper, testTimeSource, journal, testName.getMethodName());
  }

  static class TestTransactionManager implements TransactionManager {

    volatile TestTransaction currentTransaction;
    final AtomicLong gtridGenerator = new AtomicLong();

    public TestTransaction getCurrentTransaction() {
      return currentTransaction;
    }

    @Override
    public void begin() {
      currentTransaction = new TestTransaction(gtridGenerator.incrementAndGet());
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
      currentTransaction.commit();
      currentTransaction = null;
    }

    @Override
    public int getStatus() {
      return 0;
    }

    @Override
    public Transaction getTransaction() {
      return currentTransaction;
    }

    @Override
    public void resume(Transaction tobj) {

    }

    @Override
    public void rollback() throws IllegalStateException, SecurityException, SystemException {
      currentTransaction.rollback();
      currentTransaction = null;
    }

    @Override
    public void setRollbackOnly() {

    }

    @Override
    public void setTransactionTimeout(int seconds) {

    }

    @Override
    public Transaction suspend() {
      return null;
    }
  }

  static class TestTransaction implements Transaction {

    final long gtrid;
    final Map<XAResource, TestXid> xids = new IdentityHashMap<>();
    final AtomicLong bqualGenerator = new AtomicLong();
    final List<Synchronization> synchronizations = new CopyOnWriteArrayList<>();
    final List<TwoPcListener> twoPcListeners = new CopyOnWriteArrayList<>();

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

        Set<XAResource> preparedResources = new HashSet<>();

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

        fireInMiddleOf2PC();

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
    public boolean delistResource(XAResource xaRes, int flag) {
      return true;
    }

    @Override
    public boolean enlistResource(XAResource xaRes) throws IllegalStateException, SystemException {
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
    public int getStatus() {
      return 0;
    }

    public void registerTwoPcListener(TwoPcListener listener) {
      twoPcListeners.add(listener);
    }

    @Override
    public void registerSynchronization(Synchronization sync) {
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

    private void fireInMiddleOf2PC() {
      for (TwoPcListener twoPcListener : twoPcListeners) {
        twoPcListener.inMiddleOf2PC();
      }
    }

    @Override
    public void setRollbackOnly() {

    }
  }

  interface TwoPcListener {
    void inMiddleOf2PC();
  }

}
