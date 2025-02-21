/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import com.arjuna.ats.arjuna.common.arjPropertyManager;
import com.arjuna.ats.arjuna.coordinator.TransactionReaper;
import com.arjuna.ats.arjuna.coordinator.TxControl;
import jakarta.transaction.TransactionManager;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.transactions.xa.txmgr.narayana.NarayanaTransactionManagerLookup;
import org.ehcache.transactions.xa.txmgr.provider.LookupTransactionManagerProviderConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.Callable;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.terracotta.utilities.test.matchers.ThrowsMatcher.threw;

public class JakartaSmokeTest {

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    String narayanaPath = folder.newFolder("narayana").getAbsolutePath();
    arjPropertyManager.getObjectStoreEnvironmentBean().setObjectStoreDir(narayanaPath);
    arjPropertyManager.getCoordinatorEnvironmentBean().setStartDisabled(true);

    TxControl.enable();
  }

  @After
  public void tearDown() {
    TxControl.disable(true);
    TransactionReaper.terminate(false);
  }

  @Test
  public void testCommittedWriteIsReadable() throws Exception {
    TransactionManager transactionManager = com.arjuna.ats.jta.TransactionManager.transactionManager();

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .using(new LookupTransactionManagerProviderConfiguration(NarayanaTransactionManagerLookup.class))
      .withCache("xaCache", newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .withService(new XAStoreConfiguration("xaCache"))
        .build()).build(true)) {

      Cache<Long, String> xaCache = cacheManager.getCache("xaCache", Long.class, String.class);

      underTransaction(transactionManager, () -> xaCache.put(1L, "one"));

      assertThat(underTransaction(transactionManager, () -> xaCache.get(1L)), is("one"));
    }
  }

  @Test
  public void testRolledBackWriteIsRemoved() throws Exception {
    TransactionManager transactionManager = com.arjuna.ats.jta.TransactionManager.transactionManager();

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .using(new LookupTransactionManagerProviderConfiguration(NarayanaTransactionManagerLookup.class))
      .withCache("xaCache", newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .withService(new XAStoreConfiguration("xaCache")).build())
      .build(true)) {

      Cache<Long, String> xaCache = cacheManager.getCache("xaCache", Long.class, String.class);

      transactionManager.begin();
      try {
        xaCache.put(1L, "one");
      } finally {
        transactionManager.rollback();
      }

      assertThat(underTransaction(transactionManager, () -> xaCache.get(1L)), is(nullValue()));
    }
  }

  @Test
  public void testNonTransactionalAccessFails() {
    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .using(new LookupTransactionManagerProviderConfiguration(NarayanaTransactionManagerLookup.class))
      .withCache("xaCache", newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .withService(new XAStoreConfiguration("xaCache")).build())
      .build(true)) {

      Cache<Long, String> xaCache = cacheManager.getCache("xaCache", Long.class, String.class);

      assertThat(() -> xaCache.get(1L), threw(instanceOf(XACacheException.class)));
    }
  }

  @Test
  public void testXACacheWithXMLConfig() {
    URL url = this.getClass().getResource("/configs/jakarta-smoke-test.xml");
    Configuration xmlConfig = new XmlConfiguration(url);
    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManager(xmlConfig)) {
      cacheManager.init();
    }
  }

  private static void underTransaction(TransactionManager manager, Runnable runnable) throws Exception {
    underTransaction(manager, () -> {
      runnable.run();
      return null;
    });
  }

  private static <V> V underTransaction(TransactionManager manager, Callable<V> callable) throws Exception {
    manager.begin();
    try {
      V value = callable.call();
      manager.commit();
      return value;
    } catch (Throwable t) {
      manager.rollback();
      throw t;
    }
  }

}
