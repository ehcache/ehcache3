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
package org.ehcache.docs.transactions.xa;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.transactions.xa.txmgr.btm.BitronixTransactionManagerLookup;
import org.ehcache.transactions.xa.txmgr.provider.LookupTransactionManagerProviderConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.transactions.xa.XACacheException;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Ludovic Orban
 */
public class XAGettingStarted {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    TransactionManagerServices.getConfiguration().setJournal("null").setServerId(getClass().getSimpleName());
  }

  @After
  public void tearDown() throws Exception {
    if (TransactionManagerServices.isTransactionManagerRunning()) {
      TransactionManagerServices.getTransactionManager().shutdown();
    }
  }

  @Test
  public void testSimpleXACache() throws Exception {
    // tag::testSimpleXACache[]
    BitronixTransactionManager transactionManager =
        TransactionManagerServices.getTransactionManager(); // <1>

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class)) // <2>
        .withCache("xaCache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, // <3>
                                                            ResourcePoolsBuilder.heap(10)) // <4>
            .withService(new XAStoreConfiguration("xaCache")) // <5>
            .build()
        )
        .build(true);

    Cache<Long, String> xaCache = cacheManager.getCache("xaCache", Long.class, String.class);

    transactionManager.begin(); // <6>
    {
      xaCache.put(1L, "one"); // <7>
    }
    transactionManager.commit(); // <8>

    cacheManager.close();
    transactionManager.shutdown();
    // end::testSimpleXACache[]
  }

  @Test
  public void testNonTransactionalAccess() throws Exception {
    // tag::testNonTransactionalAccess[]
    BitronixTransactionManager transactionManager =
        TransactionManagerServices.getTransactionManager(); // <1>

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class)) // <2>
        .withCache("xaCache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, // <3>
                                                            ResourcePoolsBuilder.heap(10)) // <4>
            .withService(new XAStoreConfiguration("xaCache")) // <5>
            .build()
        )
        .build(true);

    Cache<Long, String> xaCache = cacheManager.getCache("xaCache", Long.class, String.class);

    try {
      xaCache.get(1L); // <6>
      fail("expected XACacheException");
    } catch (XACacheException e) {
      // expected
    }

    cacheManager.close();
    transactionManager.shutdown();
    // end::testNonTransactionalAccess[]
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testXACacheWithWriteThrough() throws Exception {
    // tag::testXACacheWithWriteThrough[]
    BitronixTransactionManager transactionManager =
        TransactionManagerServices.getTransactionManager(); // <1>

    Class<CacheLoaderWriter<?, ?>> klazz = (Class<CacheLoaderWriter<?, ?>>) (Class) (SampleLoaderWriter.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class)) // <2>
        .withCache("xaCache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, // <3>
                                                            ResourcePoolsBuilder.heap(10)) // <4>
                .withService(new XAStoreConfiguration("xaCache")) // <5>
                .withService(new DefaultCacheLoaderWriterConfiguration(klazz, singletonMap(1L, "eins"))) // <6>
                .build()
        )
        .build(true);

    Cache<Long, String> xaCache = cacheManager.getCache("xaCache", Long.class, String.class);

    transactionManager.begin(); // <7>
    {
      assertThat(xaCache.get(1L), equalTo("eins")); // <8>
      xaCache.put(1L, "one"); // <9>
    }
    transactionManager.commit(); // <10>

    cacheManager.close();
    transactionManager.shutdown();
    // end::testXACacheWithWriteThrough[]
  }

  @Test
  public void testXACacheWithThreeTiers() throws Exception {
    // tag::testXACacheWithThreeTiers[]
    BitronixTransactionManager transactionManager =
        TransactionManagerServices.getTransactionManager(); // <1>

    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class)) // <2>
        .with(CacheManagerBuilder.persistence(new File(getStoragePath(), "testXACacheWithThreeTiers"))) // <3>
        .withCache("xaCache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, // <4>
                ResourcePoolsBuilder.newResourcePoolsBuilder() // <5>
                        .heap(10, EntryUnit.ENTRIES)
                        .offheap(10, MemoryUnit.MB)
                        .disk(20, MemoryUnit.MB, true)
                )
                .withService(new XAStoreConfiguration("xaCache")) // <6>
                .build()
        )
        .build(true);

    Cache<Long, String> xaCache = persistentCacheManager.getCache("xaCache", Long.class, String.class);

    transactionManager.begin(); // <7>
    {
      xaCache.put(1L, "one"); // <8>
    }
    transactionManager.commit(); // <9>

    persistentCacheManager.close();
    transactionManager.shutdown();
    // end::testXACacheWithThreeTiers[]
  }

  @Test
  public void testXACacheWithXMLConfig() throws Exception {
    // tag::testXACacheWithXMLConfig[]
    BitronixTransactionManager transactionManager =
        TransactionManagerServices.getTransactionManager(); // <1>

    URL myUrl = this.getClass().getResource("/docs/configs/xa-getting-started.xml"); // <2>
    Configuration xmlConfig = new XmlConfiguration(myUrl); // <3>
    CacheManager myCacheManager = CacheManagerBuilder.newCacheManager(xmlConfig); // <4>
    myCacheManager.init();

    myCacheManager.close();
    transactionManager.shutdown();
    // end::testXACacheWithXMLConfig[]
  }

  private String getStoragePath() throws IOException {
    return folder.newFolder().getAbsolutePath();
  }

  public static class SampleLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleLoaderWriter.class);

    private final Map<K, V> data = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public SampleLoaderWriter(Map<K, V> initialData) {
      data.putAll(initialData);
    }

    @Override
    public V load(K key) {
      lock.readLock().lock();
      try {
        V v = data.get(key);
        LOGGER.info("Key - '{}', Value - '{}' successfully loaded", key, v);
        return v;
      } finally {
        lock.readLock().unlock();
      }
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void write(K key, V value) {
      lock.writeLock().lock();
      try {
        data.put(key, value);
        LOGGER.info("Key - '{}', Value - '{}' successfully written", key, value);
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
      lock.writeLock().lock();
      try {
        for (Map.Entry<? extends K, ? extends V> entry : entries) {
          data.put(entry.getKey(), entry.getValue());
          LOGGER.info("Key - '{}', Value - '{}' successfully written in batch", entry.getKey(), entry.getValue());
        }
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public void delete(K key) {
      lock.writeLock().lock();
      try {
        data.remove(key);
        LOGGER.info("Key - '{}' successfully deleted", key);
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public void deleteAll(Iterable<? extends K> keys) {
      lock.writeLock().lock();
      try {
        for (K key : keys) {
          data.remove(key);
          LOGGER.info("Key - '{}' successfully deleted in batch", key);
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

}
