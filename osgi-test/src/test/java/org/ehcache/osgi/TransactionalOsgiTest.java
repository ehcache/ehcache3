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

package org.ehcache.osgi;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.ehcache.transactions.xa.txmgr.btm.BitronixTransactionManagerLookup;
import org.ehcache.transactions.xa.txmgr.provider.LookupTransactionManagerProviderConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

import static bitronix.tm.TransactionManagerServices.getTransactionManager;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManager;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.osgi.OsgiTestUtils.baseConfiguration;
import static org.ehcache.osgi.OsgiTestUtils.gradleBundle;
import static org.ehcache.osgi.OsgiTestUtils.jaxbConfiguration;
import static org.ehcache.osgi.OsgiTestUtils.jtaConfiguration;
import static org.ehcache.osgi.OsgiTestUtils.wrappedGradleBundle;
import static org.ops4j.pax.exam.CoreOptions.options;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class TransactionalOsgiTest {

  @Configuration
  public Option[] individualModules() {
    return options(
      gradleBundle("org.ehcache.modules:api"),
      gradleBundle("org.ehcache.modules:core"),
      gradleBundle("org.ehcache.modules:impl"),
      gradleBundle("org.ehcache.modules:xml"), jaxbConfiguration(),
      gradleBundle("org.ehcache:transactions"), jtaConfiguration(),

      gradleBundle("org.terracotta.management:management-model"),
      gradleBundle("org.terracotta.management:sequence-generator"),

      wrappedGradleBundle("org.terracotta:statistics"),
      wrappedGradleBundle("org.ehcache:sizeof"),
      wrappedGradleBundle("org.terracotta:offheap-store"),
      wrappedGradleBundle("org.terracotta:terracotta-utilities-tools"),

      baseConfiguration("TransactionalOsgiTest", "individualModules")
    );
  }

  @Configuration
  public Option[] uberJar() {
    return options(
      gradleBundle("org.ehcache:dist"), jaxbConfiguration(),
      gradleBundle("org.ehcache:transactions"), jtaConfiguration(),

      baseConfiguration("TransactionalOsgiTest", "uberJar")
    );
  }

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
  public void testProgrammaticConfiguration() throws Exception {
    TestMethods.testProgrammaticConfiguration();
  }

  @Test
  public void testXmlConfiguration() throws Exception {
    TestMethods.testXmlConfiguration();
  }

  private static class TestMethods {

    public static void testProgrammaticConfiguration() throws Exception {
      BitronixTransactionManager transactionManager = getTransactionManager();

      try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withClassLoader(TestMethods.class.getClassLoader())
        .using(new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class))
        .withCache("xaCache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
          .withService(new XAStoreConfiguration("xaCache")).build()).build(true)) {

        Cache<Long, String> xaCache = cacheManager.getCache("xaCache", Long.class, String.class);

        transactionManager.begin();
        try {
          xaCache.put(1L, "one");
        } catch (Throwable t) {
          transactionManager.rollback();
        }
        transactionManager.commit();
      }
      transactionManager.shutdown();
    }

    public static void testXmlConfiguration() throws Exception {
      BitronixTransactionManager transactionManager = getTransactionManager();

      try (CacheManager cacheManager = newCacheManager(
        new XmlConfiguration(TestMethods.class.getResource("ehcache-xa-osgi.xml"), TestMethods.class.getClassLoader())
      )) {
        cacheManager.init();

        Cache<Long, String> xaCache = cacheManager.getCache("xaCache", Long.class, String.class);

        transactionManager.begin();
        try {
          xaCache.put(1L, "one");
        } catch (Throwable t) {
          transactionManager.rollback();
        }
        transactionManager.commit();
      }
      transactionManager.shutdown();
    }
  }
}
