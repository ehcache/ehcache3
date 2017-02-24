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

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;

import java.net.URI;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class XA107Test {

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
  public void testXAWorksWithJsr107() throws Exception {
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();
    URI uri = getClass().getResource("/configs/simple-xa.xml").toURI();
    CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(uri, getClass().getClassLoader());

    Cache<String, String> xaCache = cacheManager.getCache("xaCache", String.class, String.class);

    transactionManager.begin();
    {
        xaCache.put("key", "one");
    }
    transactionManager.commit();

    cacheManager.close();
  }

}
