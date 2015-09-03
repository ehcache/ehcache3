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
import org.ehcache.transactions.configuration.DefaultTxService;
import org.ehcache.transactions.configuration.TxCacheManagerConfiguration;
import org.ehcache.transactions.configuration.TxServiceConfiguration;
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

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("txCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().add(new TxServiceConfiguration()).buildConfig(Long.class, String.class))
        .with(new TxCacheManagerConfiguration())
        .using(new DefaultTxService(transactionManager))
        .build(true);

    Cache<Long, String> txCache = cacheManager.getCache("txCache", Long.class, String.class);

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
