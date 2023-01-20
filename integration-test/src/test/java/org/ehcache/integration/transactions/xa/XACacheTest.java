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
package org.ehcache.integration.transactions.xa;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import bitronix.tm.internal.TransactionStatusChangeListener;
import bitronix.tm.recovery.Recoverer;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.impl.internal.DefaultTimeSourceService;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.ehcache.spi.copy.Copier;
import org.ehcache.transactions.xa.XACacheException;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.ehcache.transactions.xa.txmgr.btm.BitronixTransactionManagerLookup;
import org.ehcache.transactions.xa.txmgr.provider.LookupTransactionManagerProviderConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Transaction;
import java.io.File;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Ludovic Orban
 */
public class XACacheTest {

  @Before
  public void setUpBtmConfig() throws Exception {
    TransactionManagerServices.getConfiguration()
        .setLogPart1Filename(getStoragePath() + "/btm1.tlog")
        .setLogPart2Filename(getStoragePath() + "/btm2.tlog")
        .setServerId("XACacheTest")
        .setGracefulShutdownInterval(0);
  }

  @After
  public void tearDownBtm() throws Exception {
    if (TransactionManagerServices.isTransactionManagerRunning()) {
      TransactionManagerServices.getTransactionManager().shutdown();
    }
  }

  @Test
  public void testEndToEnd() throws Exception {
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();

    CacheConfigurationBuilder<Long, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(10, MemoryUnit.MB));

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class))
        .withCache("txCache1", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache1")).build())
        .withCache("txCache2", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache2")).build())
        .withCache("nonTxCache", cacheConfigurationBuilder.build())
        .build(true);

    final Cache<Long, String> txCache1 = cacheManager.getCache("txCache1", Long.class, String.class);
    final Cache<Long, String> txCache2 = cacheManager.getCache("txCache2", Long.class, String.class);
    Cache<Long, String> nonTxCache = cacheManager.getCache("nonTxCache", Long.class, String.class);

    nonTxCache.put(1L, "eins");
    assertThat(nonTxCache.get(1L), equalTo("eins"));

    try {
      txCache1.put(1L, "one");
      fail("expected XACacheException");
    } catch (XACacheException e) {
      // expected
    }

    transactionManager.begin();
    {
      txCache1.put(1L, "one");
    }
    transactionManager.commit();

    transactionManager.begin();
    {
      txCache1.get(1L);
      txCache2.get(1L);
    }
    transactionManager.commit();

    transactionManager.begin();
    {
      String s = txCache1.get(1L);
      assertThat(s, equalTo("one"));
      txCache1.remove(1L);

      Transaction suspended = transactionManager.suspend();
      transactionManager.begin();
      {
        txCache2.put(1L, "uno");
        String s2 = txCache1.get(1L);
        assertThat(s2, equalTo("one"));
      }
      transactionManager.commit();
      transactionManager.resume(suspended);

      String s1 = txCache2.get(1L);
      assertThat(s1, equalTo("uno"));

    }
    transactionManager.commit();

    cacheManager.close();
    transactionManager.shutdown();
  }

  @Test
  public void testRecoveryWithInflightTx() throws Exception {
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();

    CacheConfigurationBuilder<Long, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(10, MemoryUnit.MB));

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("txCache1", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache1")).build())
        .withCache("txCache2", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache2")).build())
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class))
        .build(true);

    final Cache<Long, String> txCache1 = cacheManager.getCache("txCache1", Long.class, String.class);
    final Cache<Long, String> txCache2 = cacheManager.getCache("txCache2", Long.class, String.class);

    transactionManager.begin();
    {
      txCache1.put(1L, "one");
      txCache2.put(1L, "un");
    }
    transactionManager.commit();


    transactionManager.begin();
    {
      txCache1.remove(1L);
      txCache2.remove(1L);
    }
    transactionManager.getCurrentTransaction().addTransactionStatusChangeListener(new TransactionStatusChangeListener() {
      @Override
      public void statusChanged(int oldStatus, int newStatus) {
        if (newStatus == Status.STATUS_PREPARED) {
          Recoverer recoverer = TransactionManagerServices.getRecoverer();
          recoverer.run();
          assertThat(recoverer.getCommittedCount(), is(0));
          assertThat(recoverer.getRolledbackCount(), is(0));
        }
      }
    });
    transactionManager.commit();

    cacheManager.close();
    transactionManager.shutdown();
  }

  @Test
  public void testRecoveryAfterCrash() throws Exception {
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();

    CacheConfigurationBuilder<Long, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(10, MemoryUnit.MB)
                .disk(20, MemoryUnit.MB, true));

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath())))
        .withCache("txCache1", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache1")).build())
        .withCache("txCache2", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache2")).build())
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class))
        .build(true);

    Cache<Long, String> txCache1 = cacheManager.getCache("txCache1", Long.class, String.class);
    Cache<Long, String> txCache2 = cacheManager.getCache("txCache2", Long.class, String.class);

    transactionManager.begin();
    {
      txCache1.put(1L, "one");
      txCache2.put(1L, "un");
    }
    transactionManager.getCurrentTransaction().addTransactionStatusChangeListener(new TransactionStatusChangeListener() {
      @Override
      public void statusChanged(int oldStatus, int newStatus) {
        if (newStatus == Status.STATUS_COMMITTING) {
          throw new AbortError();
        }
      }
    });
    try {
      transactionManager.commit();
      fail("expected AbortError");
    } catch (AbortError e) {
      // expected
    }

    cacheManager.close();
    txCache1 = null;
    txCache2 = null;
    transactionManager.shutdown();

    setUpBtmConfig();
    transactionManager = TransactionManagerServices.getTransactionManager();
    cacheManager.init();

    txCache1 = cacheManager.getCache("txCache1", Long.class, String.class);
    txCache2 = cacheManager.getCache("txCache2", Long.class, String.class);

    transactionManager.begin();
    {
      assertThat(txCache1.get(1L), equalTo("one"));
      assertThat(txCache2.get(1L), equalTo("un"));
    }
    transactionManager.commit();

    cacheManager.close();
    transactionManager.shutdown();
  }

  static class AbortError extends Error {
  }

  @Test
  public void testExpiry() throws Exception {
    TestTimeSource testTimeSource = new TestTimeSource();
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();

    CacheConfigurationBuilder<Long, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder()
                        .heap(10, EntryUnit.ENTRIES)
                        .offheap(10, MemoryUnit.MB))
        .withExpiry(Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.SECONDS)));

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("txCache1", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache1")).build())
        .withCache("txCache2", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache2")).build())
        .using(new DefaultTimeSourceService(new TimeSourceConfiguration(testTimeSource)))
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class))
        .build(true);

    final Cache<Long, String> txCache1 = cacheManager.getCache("txCache1", Long.class, String.class);
    final Cache<Long, String> txCache2 = cacheManager.getCache("txCache2", Long.class, String.class);

    transactionManager.begin();
    {
      txCache1.put(1L, "one");
      txCache2.put(1L, "un");
    }
    transactionManager.commit();


    transactionManager.begin();
    {
      txCache1.put(1L, "eins");
      txCache2.put(1L, "uno");
    }
    transactionManager.commit();

    testTimeSource.advanceTime(2000);

    transactionManager.begin();
    {
      assertThat(txCache1.get(1L), is(nullValue()));
      assertThat(txCache2.get(1L), is(nullValue()));
    }
    transactionManager.commit();


    cacheManager.close();
    transactionManager.shutdown();
  }

  @Test
  public void testCopiers() throws Exception {
    TestTimeSource testTimeSource = new TestTimeSource();
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();

    CacheConfigurationBuilder<Long, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(10, MemoryUnit.MB)
                .disk(20, MemoryUnit.MB, true));

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath())))
        .withCache("txCache1", cacheConfigurationBuilder
                .add(new XAStoreConfiguration("txCache1"))
                .add(new DefaultCopierConfiguration<Long>(LongCopier.class, DefaultCopierConfiguration.Type.KEY))
                .add(new DefaultCopierConfiguration<String>(StringCopier.class, DefaultCopierConfiguration.Type.VALUE))
                .build()
        )
        .withCache("txCache2", cacheConfigurationBuilder
            .add(new XAStoreConfiguration("txCache2"))
            .add(new DefaultCopierConfiguration<Long>(LongCopier.class, DefaultCopierConfiguration.Type.KEY))
            .add(new DefaultCopierConfiguration<String>(StringCopier.class, DefaultCopierConfiguration.Type.VALUE))
            .build())
        .using(new DefaultTimeSourceService(new TimeSourceConfiguration(testTimeSource)))
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class))
        .build(true);

    final Cache<Long, String> txCache1 = cacheManager.getCache("txCache1", Long.class, String.class);
    final Cache<Long, String> txCache2 = cacheManager.getCache("txCache2", Long.class, String.class);

    transactionManager.begin();
    {
      txCache1.put(1L, "one");
      txCache2.put(1L, "un");
    }
    transactionManager.commit();


    transactionManager.begin();
    {
      txCache1.put(1L, "eins");
      txCache2.put(1L, "uno");
    }
    transactionManager.commit();


    transactionManager.begin();
    {
      assertThat(txCache1.get(1L), equalTo("eins"));
      assertThat(txCache2.get(1L), equalTo("uno"));
    }
    transactionManager.commit();


    cacheManager.close();
    transactionManager.shutdown();
  }

  @Test
  public void testTimeout() throws Exception {
    TestTimeSource testTimeSource = new TestTimeSource();
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();

    CacheConfigurationBuilder<Long, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(10, MemoryUnit.MB));

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath())))
        .withCache("txCache1", cacheConfigurationBuilder
                .add(new XAStoreConfiguration("txCache1"))
                .add(new DefaultCopierConfiguration<Long>(LongCopier.class, DefaultCopierConfiguration.Type.KEY))
                .add(new DefaultCopierConfiguration<String>(StringCopier.class, DefaultCopierConfiguration.Type.VALUE))
                .build()
        )
        .withCache("txCache2", cacheConfigurationBuilder
            .add(new XAStoreConfiguration("txCache2"))
            .add(new DefaultCopierConfiguration<Long>(LongCopier.class, DefaultCopierConfiguration.Type.KEY))
            .add(new DefaultCopierConfiguration<String>(StringCopier.class, DefaultCopierConfiguration.Type.VALUE))
            .build())
        .using(new DefaultTimeSourceService(new TimeSourceConfiguration(testTimeSource)))
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class))
        .build(true);

    final Cache<Long, String> txCache1 = cacheManager.getCache("txCache1", Long.class, String.class);
    final Cache<Long, String> txCache2 = cacheManager.getCache("txCache2", Long.class, String.class);

    transactionManager.setTransactionTimeout(1);
    transactionManager.begin();
    {
      txCache1.put(1L, "one");
      txCache2.put(1L, "un");
      testTimeSource.advanceTime(2000);
    }
    try {
      transactionManager.commit();
      fail("Expected RollbackException");
    } catch (RollbackException e) {
      // expected
    }

    transactionManager.setTransactionTimeout(1);
    transactionManager.begin();
    {
      txCache1.put(1L, "one");
      txCache2.put(1L, "un");
      testTimeSource.advanceTime(2000);
      try {
        txCache2.put(1L, "uno");
        fail("expected XACacheException");
      } catch (XACacheException e) {
        // expected
      }
    }
    try {
      transactionManager.commit();
      fail("Expected RollbackException");
    } catch (RollbackException e) {
      // expected
    }

    cacheManager.close();
    transactionManager.shutdown();
  }


  @Test
  public void testConcurrentTx() throws Exception {
    TestTimeSource testTimeSource = new TestTimeSource();
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();

    CacheConfigurationBuilder<Long, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder()
                        .heap(10, EntryUnit.ENTRIES)
                        .offheap(10, MemoryUnit.MB))
        .withExpiry(Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.SECONDS)));

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("txCache1", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache1")).build())
        .withCache("txCache2", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache2")).build())
        .using(new DefaultTimeSourceService(new TimeSourceConfiguration(testTimeSource)))
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class))
        .build(true);

    final Cache<Long, String> txCache1 = cacheManager.getCache("txCache1", Long.class, String.class);
    final Cache<Long, String> txCache2 = cacheManager.getCache("txCache2", Long.class, String.class);

    CyclicBarrier barrier = new CyclicBarrier(2);

    TxThread tx1 = new TxThread(transactionManager, barrier) {
      @Override
      public void doTxWork() throws Exception {
        Thread.currentThread().setName("tx1");
        txCache1.put(0L, "zero");
        txCache1.put(1L, "one");
        txCache2.put(-1L, "-one");
        txCache2.put(-2L, "-two");
      }
    };
    TxThread tx2 = new TxThread(transactionManager, barrier) {
      @Override
      public void doTxWork() throws Exception {
        Thread.currentThread().setName("tx2");
        txCache1.put(1L, "un");
        txCache1.put(2L, "deux");
        txCache2.put(-1L, "-un");
        txCache2.put(-3L, "-trois");
      }
    };
    tx1.start();
    tx2.start();
    tx1.join();
    tx2.join();

    if (tx1.ex != null) {
      System.err.println("tx1 error");
      tx1.ex.printStackTrace();
    }
    if (tx2.ex != null) {
      System.err.println("tx2 error");
      tx2.ex.printStackTrace();
    }

    transactionManager.begin();
    assertThat(txCache1.get(0L), equalTo("zero"));
    assertThat(txCache1.get(1L), is(nullValue()));
    assertThat(txCache1.get(2L), equalTo("deux"));
    assertThat(txCache2.get(-1L), is(nullValue()));
    assertThat(txCache2.get(-2L), equalTo("-two"));
    assertThat(txCache2.get(-3L), equalTo("-trois"));
    transactionManager.commit();

    cacheManager.close();
    transactionManager.shutdown();
  }

  @Test
  public void testAtomicsWithoutLoaderWriter() throws Exception {
    TestTimeSource testTimeSource = new TestTimeSource();
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();

    CacheConfigurationBuilder<Long, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder()
                    .heap(10, EntryUnit.ENTRIES)
                    .offheap(10, MemoryUnit.MB)
                )
        .withExpiry(Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.SECONDS)));

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("txCache1", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache1")).build())
        .using(new DefaultTimeSourceService(new TimeSourceConfiguration(testTimeSource)))
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class))
        .build(true);

    final Cache<Long, String> txCache1 = cacheManager.getCache("txCache1", Long.class, String.class);

    putIfAbsentAssertions(transactionManager, txCache1);
    txCache1.clear();
    remove2ArgsAssertions(transactionManager, txCache1);
    txCache1.clear();
    replace2ArgsAssertions(transactionManager, txCache1);
    txCache1.clear();
    replace3ArgsAssertions(transactionManager, txCache1);
    txCache1.clear();

    cacheManager.close();
    transactionManager.shutdown();
  }

  @Test
  public void testAtomicsWithLoaderWriter() throws Exception {
    TestTimeSource testTimeSource = new TestTimeSource();
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();
    SampleLoaderWriter<Long, String> loaderWriter = new SampleLoaderWriter<Long, String>();

    CacheConfigurationBuilder<Long, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder()
                    .heap(10, EntryUnit.ENTRIES)
                    .offheap(10, MemoryUnit.MB))
        .withExpiry(Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.SECONDS)))
        .withLoaderWriter(loaderWriter);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("txCache1", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache1")).build())
        .using(new DefaultTimeSourceService(new TimeSourceConfiguration(testTimeSource)))
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class))
        .build(true);

    final Cache<Long, String> txCache1 = cacheManager.getCache("txCache1", Long.class, String.class);

    putIfAbsentAssertions(transactionManager, txCache1);
    txCache1.clear();
    loaderWriter.clear();
    remove2ArgsAssertions(transactionManager, txCache1);
    txCache1.clear();
    loaderWriter.clear();
    replace2ArgsAssertions(transactionManager, txCache1);
    txCache1.clear();
    loaderWriter.clear();
    replace3ArgsAssertions(transactionManager, txCache1);
    txCache1.clear();
    loaderWriter.clear();

    cacheManager.close();
    transactionManager.shutdown();
  }

  private void putIfAbsentAssertions(BitronixTransactionManager transactionManager, Cache<Long, String> txCache1) throws Exception {
    transactionManager.begin();
    {
      assertThat(txCache1.putIfAbsent(1L, "one"), is(nullValue()));
      assertThat(txCache1.putIfAbsent(1L, "un"), equalTo("one"));
    }
    transactionManager.commit();

    assertMapping(transactionManager, txCache1, 1L, "one");

    transactionManager.begin();
    {
      assertThat(txCache1.putIfAbsent(1L, "eins"), equalTo("one"));
      txCache1.remove(1L);
      assertThat(txCache1.putIfAbsent(1L, "een"), is(nullValue()));
    }
    transactionManager.commit();

    assertMapping(transactionManager, txCache1, 1L, "een");
  }

  private void remove2ArgsAssertions(BitronixTransactionManager transactionManager, Cache<Long, String> txCache1) throws Exception {
    transactionManager.begin();
    {
      assertThat(txCache1.remove(1L, "one"), is(false));
      assertThat(txCache1.putIfAbsent(1L, "un"), is(nullValue()));
      assertThat(txCache1.remove(1L, "one"), is(false));
      assertThat(txCache1.remove(1L, "un"), is(true));
      assertThat(txCache1.remove(1L, "un"), is(false));
    }
    transactionManager.commit();

    assertMapping(transactionManager, txCache1, 1L, null);

    transactionManager.begin();
    {
      txCache1.put(1L, "one");
    }
    transactionManager.commit();

    assertMapping(transactionManager, txCache1, 1L, "one");

    transactionManager.begin();
    {
      assertThat(txCache1.remove(1L, "un"), is(false));
      assertThat(txCache1.remove(1L, "one"), is(true));
      assertThat(txCache1.remove(1L, "one"), is(false));
      assertThat(txCache1.putIfAbsent(1L, "un"), is(nullValue()));
      assertThat(txCache1.remove(1L, "one"), is(false));
      assertThat(txCache1.remove(1L, "un"), is(true));
    }
    transactionManager.commit();

    assertMapping(transactionManager, txCache1, 1L, null);
  }

  private void replace2ArgsAssertions(BitronixTransactionManager transactionManager, Cache<Long, String> txCache1) throws Exception {
    transactionManager.begin();
    {
      assertThat(txCache1.replace(1L, "one"), is(nullValue()));
      txCache1.put(1L, "un");
      assertThat(txCache1.replace(1L, "eins"), equalTo("un"));
    }
    transactionManager.commit();

    assertMapping(transactionManager, txCache1, 1L, "eins");

    transactionManager.begin();
    {
      assertThat(txCache1.replace(1L, "een"), equalTo("eins"));
      txCache1.put(1L, "un");
      assertThat(txCache1.replace(1L, "een"), equalTo("un"));
      txCache1.remove(1L);
      assertThat(txCache1.replace(1L, "one"), is(nullValue()));
      assertThat(txCache1.get(1L), is(nullValue()));
    }
    transactionManager.commit();

    assertMapping(transactionManager, txCache1, 1L, null);
  }

  private void replace3ArgsAssertions(BitronixTransactionManager transactionManager, Cache<Long, String> txCache1) throws Exception {
    transactionManager.begin();
    {
      assertThat(txCache1.replace(1L, "one", "un"), is(false));
      txCache1.put(1L, "un");
      assertThat(txCache1.replace(1L, "uno", "eins"), is(false));
      assertThat(txCache1.replace(1L, "un", "eins"), is(true));
      assertThat(txCache1.get(1L), equalTo("eins"));
    }
    transactionManager.commit();

    assertMapping(transactionManager, txCache1, 1L, "eins");

    transactionManager.begin();
    {
      assertThat(txCache1.replace(1L, "one", "un"), is(false));
      assertThat(txCache1.replace(1L, "eins", "un"), is(true));
      assertThat(txCache1.replace(1L, "uno", "een"), is(false));
      assertThat(txCache1.get(1L), equalTo("un"));
    }
    transactionManager.commit();

    assertMapping(transactionManager, txCache1, 1L, "un");
  }

  private void assertMapping(BitronixTransactionManager transactionManager, Cache<Long, String> cache, long key, String expected) throws Exception {
    transactionManager.begin();

    String value = cache.get(key);
    if (expected == null) {
      assertThat(value, is(nullValue()));
    } else {
      assertThat(value, equalTo(expected));
    }

    transactionManager.commit();
  }

  @Test
  public void testIterate() throws Throwable {
    TestTimeSource testTimeSource = new TestTimeSource();
    final BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();

    CacheConfigurationBuilder<Long, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        newResourcePoolsBuilder()
                    .heap(10, EntryUnit.ENTRIES)
                    .offheap(10, MemoryUnit.MB))
        .withExpiry(Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.SECONDS)));

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("txCache1", cacheConfigurationBuilder.add(new XAStoreConfiguration("txCache1")).build())
        .using(new DefaultTimeSourceService(new TimeSourceConfiguration(testTimeSource)))
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class))
        .build(true);

    final Cache<Long, String> txCache1 = cacheManager.getCache("txCache1", Long.class, String.class);

    transactionManager.begin();
    {
      txCache1.put(1L, "one");
      txCache1.put(2L, "two");

      Map<Long, String> result = new HashMap<Long, String>();
      Iterator<Cache.Entry<Long, String>> iterator = txCache1.iterator();
      while (iterator.hasNext()) {
        Cache.Entry<Long, String> next = iterator.next();
        result.put(next.getKey(), next.getValue());
        iterator.remove();
      }
      assertThat(result.size(), equalTo(2));
      assertThat(result.keySet(), containsInAnyOrder(1L, 2L));
    }
    transactionManager.commit();

    transactionManager.begin();
    {
      Map<Long, String> result = new HashMap<Long, String>();
      for (Cache.Entry<Long, String> next : txCache1) {
        result.put(next.getKey(), next.getValue());
      }
      assertThat(result.size(), equalTo(0));
    }
    transactionManager.commit();

    transactionManager.begin();
    {
      final AtomicReference<Throwable> throwableRef = new AtomicReference<Throwable>();
      txCache1.put(1L, "one");
      txCache1.put(2L, "two");

      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            transactionManager.begin();
            Map<Long, String> result = new HashMap<Long, String>();
            for (Cache.Entry<Long, String> next : txCache1) {
              result.put(next.getKey(), next.getValue());
            }
            assertThat(result.size(), equalTo(0));
            transactionManager.commit();
          } catch (Throwable t) {
            throwableRef.set(t);
          }
        }
      };
      t.start();
      t.join();

      if (throwableRef.get() != null) {
        throw throwableRef.get();
      }

    }
    transactionManager.commit();

    transactionManager.begin();
    {
      Map<Long, String> result = new HashMap<Long, String>();
      Iterator<Cache.Entry<Long, String>> iterator = txCache1.iterator();
      while (iterator.hasNext()) {
        Cache.Entry<Long, String> next = iterator.next();
        iterator.remove();
        result.put(next.getKey(), next.getValue());
      }
      assertThat(result.size(), equalTo(2));
      assertThat(result.keySet(), containsInAnyOrder(1L, 2L));
    }
    transactionManager.commit();

    transactionManager.begin();
    {
      Map<Long, String> result = new HashMap<Long, String>();
      for (Cache.Entry<Long, String> next : txCache1) {
        result.put(next.getKey(), next.getValue());
      }
      assertThat(result.size(), equalTo(0));
    }
    transactionManager.commit();

    cacheManager.close();
    transactionManager.shutdown();
  }

  static abstract class TxThread extends Thread {

    protected final BitronixTransactionManager transactionManager;
    protected final CyclicBarrier barrier;
    protected volatile Throwable ex;

    public TxThread(BitronixTransactionManager transactionManager, CyclicBarrier barrier) {
      this.transactionManager = transactionManager;
      this.barrier = barrier;
    }

    @Override
    public final void run() {
      try {
        transactionManager.begin();
        transactionManager.getCurrentTransaction().addTransactionStatusChangeListener(new TransactionStatusChangeListener() {
          @Override
          public void statusChanged(int oldStatus, int newStatus) {
            if (oldStatus == Status.STATUS_PREPARED) {
              try {
                barrier.await(5L, TimeUnit.SECONDS);
              } catch (Exception e) {
                throw new AssertionError();
              }
            }
          }
        });
        doTxWork();
        transactionManager.commit();
      } catch (Throwable t) {
        this.ex = t;
      }
    }

    public abstract void doTxWork() throws Exception;

  }

  public static class LongCopier implements Copier<Long> {
    @Override
    public Long copyForRead(Long obj) {
      return obj;
    }

    @Override
    public Long copyForWrite(Long obj) {
      return obj;
    }
  }

  public static class StringCopier implements Copier<String> {
    @Override
    public String copyForRead(String obj) {
      return obj;
    }

    @Override
    public String copyForWrite(String obj) {
      return obj;
    }
  }

  static class TestTimeSource implements TimeSource {

    private long time = 0;

    public TestTimeSource() {
    }

    public TestTimeSource(final long time) {
      this.time = time;
    }

    @Override
    public long getTimeMillis() {
      return time;
    }

    public void advanceTime(long delta) {
      this.time += delta;
    }
  }

  private String getStoragePath() throws URISyntaxException {
    return getClass().getClassLoader().getResource(".").toURI().getPath();
  }

}
