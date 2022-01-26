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

package org.ehcache.transactions.xa.integration;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.ehcache.transactions.xa.txmgr.btm.BitronixTransactionManagerLookup;
import org.ehcache.transactions.xa.txmgr.provider.LookupTransactionManagerProviderConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertNotNull;

/**
 * StatefulSerializerTest
 */
public class StatefulSerializerTest {

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
  public void testXAWithStatefulSerializer() throws Exception {
    BitronixTransactionManager manager = TransactionManagerServices.getTransactionManager();
    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .using(new LookupTransactionManagerProviderConfiguration(
            BitronixTransactionManagerLookup.class))
          .withCache("xaCache",
            CacheConfigurationBuilder
              .newCacheConfigurationBuilder(Long.class, Person.class,
                ResourcePoolsBuilder.heap(5))
              .withExpiry(ExpiryPolicyBuilder.noExpiration()).withService(new XAStoreConfiguration("xaCache"))
              .build())
          .build(true)) {

      Cache<Long, Person> cache = cacheManager.getCache("xaCache", Long.class, Person.class);
      manager.begin();
      cache.put(1L, new Person("James", 42));
      manager.commit();

      manager.begin();
      assertNotNull(cache.get(1L));
      manager.commit();
    } finally {
      manager.shutdown();
    }
  }

  public static class Person implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String name;
    public final int age;

    public Person(String name, int age) {
      if (name == null) {
        throw new NullPointerException("Name cannot be null");
      }
      this.name = name;
      this.age = age;
    }

    @Override
    public int hashCode() {
      return name.hashCode() + 31 * age;
    }

    @Override
    public boolean equals(Object obj) {
      if (! (obj instanceof Person)) {
        return false;
      }
      Person other = (Person) obj;
      return other.name.equals(name) && other.age == age;
    }
  }
}
