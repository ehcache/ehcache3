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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import static org.junit.Assert.assertEquals;
import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;

/**
 * Jsr107OsgiTest
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class Jsr107OsgiTest {

  @Configuration
  public Option[] config() {
    return options(
        mavenBundle("org.slf4j", "slf4j-api", System.getProperty("ehcache.osgi.slf4j.version")),
        mavenBundle("org.slf4j", "slf4j-simple", System.getProperty("ehcache.osgi.slf4j.version")).noStart(),
        bundle("file:" + System.getProperty("ehcache.osgi.jar")),
        mavenBundle("javax.cache", "cache-api", System.getProperty("ehcache.osgi.jcache.version")),
        junitBundles()
    );
  }

  @Test
  @Ignore("Needs https://github.com/jsr107/jsr107spec/issues/326 to be fixed and so will wait on javax.cache:cache-api:1.0.1 only")
  public void testJsr107EhcacheOsgi() throws Exception {
    CachingProvider cachingProvider = Caching.getCachingProvider("org.ehcache.jsr107.EhcacheCachingProvider", getClass().getClassLoader());
    CacheManager cacheManager = cachingProvider.getCacheManager(getClass().getResource("/org/ehcache/osgi/ehcache-107-osgi.xml").toURI(), getClass().getClassLoader());
    Cache<Long, Person> personCache = cacheManager.getCache("personCache", Long.class, Person.class);
    assertEquals(Person.class, personCache.getConfiguration(javax.cache.configuration.Configuration.class).getValueType());
  }

}
