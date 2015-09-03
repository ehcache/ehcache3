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
package org.ehcache.transactions;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.transactions.configuration.XACacheManagerConfiguration;
import org.ehcache.transactions.configuration.XAServiceConfiguration;
import org.junit.Test;

import javax.transaction.Transaction;

/**
 * @author Ludovic Orban
 */
public class XACacheTest {

  @Test
  public void testEndToEnd() throws Exception {
    TransactionManagerServices.getConfiguration().setJournal("null").setServerId("XACacheTest");
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();

    CacheConfigurationBuilder<Object, Object> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(10, MemoryUnit.MB)
        );

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("txCache", cacheConfigurationBuilder.add(new XAServiceConfiguration()).buildConfig(Long.class, String.class))
        .withCache("nonTxCache", cacheConfigurationBuilder.buildConfig(Long.class, String.class))
        .with(new XACacheManagerConfiguration())
        .build(true);

    Cache<Long, String> txCache = cacheManager.getCache("txCache", Long.class, String.class);
    Cache<Long, String> nonTxCache = cacheManager.getCache("nonTxCache", Long.class, String.class);

    nonTxCache.put(1L, "eins");
    System.out.println(nonTxCache.get(1L));

    transactionManager.begin();
    {
      txCache.put(1L, "one");
    }
    transactionManager.commit();

    transactionManager.begin();
    {
      String s = txCache.get(1L);
      System.out.println(s);
      txCache.remove(1L);

      Transaction suspended = transactionManager.suspend();
      transactionManager.begin();
      {
        String s2 = txCache.get(1L);
        System.out.println(s2);
      }
      transactionManager.commit();
      transactionManager.resume(suspended);

    }
    transactionManager.commit();



    cacheManager.close();

    transactionManager.shutdown();
  }

}
