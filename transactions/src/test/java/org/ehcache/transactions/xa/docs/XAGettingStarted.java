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
package org.ehcache.transactions.xa.docs;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.Configuration;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.config.xml.XmlConfiguration;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.transactions.xa.configuration.TransactionManagerProviderConfiguration;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.ehcache.transactions.xa.txmgrs.TransactionManagerWrapper;
import org.ehcache.transactions.xa.txmgrs.btm.BitronixXAResourceRegistry;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author Ludovic Orban
 */
public class XAGettingStarted {

  @Before
  public void setUp() throws Exception {
    TransactionManagerServices.getConfiguration().setJournal("null").setServerId(getClass().getSimpleName());
  }

  @Test
  public void testSimpleXACache() throws Exception {
    // tag::testSimpleXACache[]
    BitronixTransactionManager transactionManager =
        TransactionManagerServices.getTransactionManager(); // <1>

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("xaCache", CacheConfigurationBuilder.newCacheConfigurationBuilder() // <2>
            .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder() // <3>
                    .heap(10, EntryUnit.ENTRIES)
            )
            .add(new XAStoreConfiguration("xaCache")) // <4>
            .buildConfig(Long.class, String.class)
        )
        .build(true);

    final Cache<Long, String> xaCache = cacheManager.getCache("xaCache", Long.class, String.class);

    transactionManager.begin(); // <5>
    {
      xaCache.put(1L, "one"); // <6>
    }
    transactionManager.commit(); // <7>

    cacheManager.close();
    transactionManager.shutdown();
    // end::testSimpleXACache[]
  }

  @Test
  public void testXACacheWithSpecificJtaTm() throws Exception {
    // tag::testXACacheWithSpecificJtaTm[]
    BitronixTransactionManager transactionManager =
        TransactionManagerServices.getTransactionManager(); // <1>

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("xaCache", CacheConfigurationBuilder.newCacheConfigurationBuilder() // <2>
            .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder() // <3>
                    .heap(10, EntryUnit.ENTRIES)
            )
            .add(new XAStoreConfiguration("xaCache")) // <4>
            .buildConfig(Long.class, String.class)
        )
        .using(new TransactionManagerProviderConfiguration(
            new TransactionManagerWrapper(transactionManager, new BitronixXAResourceRegistry()))) // <5>
        .build(true);

    final Cache<Long, String> xaCache = cacheManager.getCache("xaCache", Long.class, String.class);

    transactionManager.begin(); // <6>
    {
      xaCache.put(1L, "one"); // <7>
    }
    transactionManager.commit(); // <8>

    cacheManager.close();
    transactionManager.shutdown();
    // end::testXACacheWithSpecificJtaTm[]
  }

  @Test
  public void testXACacheWithWriteThrough() throws Exception {
    // tag::testXACacheWithWriteThrough[]
    BitronixTransactionManager transactionManager =
        TransactionManagerServices.getTransactionManager(); // <1>

    Class<CacheLoaderWriter<?, ?>> klazz = (Class<CacheLoaderWriter<?, ?>>) (Class) (SampleLoaderWriter.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("xaCache", CacheConfigurationBuilder.newCacheConfigurationBuilder() // <2>
                .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder() // <3>
                        .heap(10, EntryUnit.ENTRIES)
                )
                .add(new XAStoreConfiguration("xaCache")) // <4>
                .add(new DefaultCacheLoaderWriterConfiguration(klazz, singletonMap(1L, "eins"))) // <5>
                .buildConfig(Long.class, String.class)
        )
        .build(true);

    final Cache<Long, String> xaCache = cacheManager.getCache("xaCache", Long.class, String.class);

    transactionManager.begin(); // <6>
    {
      assertThat(xaCache.get(1L), equalTo("eins")); // <7>
      xaCache.put(1L, "one"); // <8>
    }
    transactionManager.commit(); // <9>

    cacheManager.close();
    transactionManager.shutdown();
    // end::testXACacheWithWriteThrough[]
  }

  @Test
  public void testXACacheWithThreeTiers() throws Exception {
    // tag::testXACacheWithThreeTiers[]
    BitronixTransactionManager transactionManager =
        TransactionManagerServices.getTransactionManager(); // <1>

    Class<CacheLoaderWriter<?, ?>> klazz = (Class<CacheLoaderWriter<?, ?>>) (Class) (SampleLoaderWriter.class);

    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath(), "testSimpleXACacheWithThreeTiers"))) // <2>
        .withCache("xaCache", CacheConfigurationBuilder.newCacheConfigurationBuilder() // <3>
                .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder() // <4>
                        .heap(10, EntryUnit.ENTRIES)
                        .offheap(10, MemoryUnit.MB)
                        .disk(20, MemoryUnit.MB, true)
                )
                .add(new XAStoreConfiguration("xaCache")) // <5>
                .add(new DefaultCacheLoaderWriterConfiguration(klazz, singletonMap(1L, "eins"))) // <6>
                .buildConfig(Long.class, String.class)
        )
        .build(true);

    final Cache<Long, String> xaCache = persistentCacheManager.getCache("xaCache", Long.class, String.class);

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

  private String getStoragePath() throws URISyntaxException {
    return getClass().getClassLoader().getResource(".").toURI().getPath();
  }


  public static class SampleLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleLoaderWriter.class);

    private final Map<K, V> data = new HashMap<K, V>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public SampleLoaderWriter(Map<K, V> initialData) {
      data.putAll(initialData);
    }

    @Override
    public V load(K key) throws Exception {
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
    public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void write(K key, V value) throws Exception {
      lock.writeLock().lock();
      try {
        data.put(key, value);
        LOGGER.info("Key - '{}', Value - '{}' successfully written", key, value);
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws BulkCacheWritingException, Exception {
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
    public void delete(K key) throws Exception {
      lock.writeLock().lock();
      try {
        data.remove(key);
        LOGGER.info("Key - '{}' successfully deleted", key);
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public void deleteAll(Iterable<? extends K> keys) throws BulkCacheWritingException, Exception {
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
