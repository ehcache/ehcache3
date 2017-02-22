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
