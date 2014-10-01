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

package org.ehcache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheManagerConfiguration;
import org.ehcache.function.Predicate;
import org.ehcache.spi.EhcachingTest;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.loader.CacheLoaderFactory;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class CacheManagerBuilderTest {

  @Test
  public void testLoadsTheEhcache() {
    final CacheManager build = newCacheManagerBuilder().build();
    assertThat(build, notNullValue());
    assertThat(EhcachingTest.getInstantiationCountAndReset(), is(1));
    assertThat(EhcachingTest.getCreationCountAndReset(), is(1));
  }

  @Test
  public void testIsExtensible() {

    final AtomicInteger counter = new AtomicInteger(0);

    final EhcachingTest.TestCacheManager cacheManager = newCacheManagerBuilder().with(new CacheManagerConfiguration<EhcachingTest.TestCacheManager>() {
      @Override
      public CacheManagerBuilder<EhcachingTest.TestCacheManager> builder(final CacheManagerBuilder<? extends CacheManager> other) {
        counter.getAndIncrement();
        return new CacheManagerBuilder<EhcachingTest.TestCacheManager>();
      }
    }).build();

    assertThat(cacheManager, notNullValue());
    assertThat(cacheManager, is(instanceOf(EhcachingTest.TestCacheManager.class)));
    assertThat(EhcachingTest.getInstantiationCountAndReset(), is(1));
    assertThat(EhcachingTest.getCreationCountAndReset(), is(1));
    assertThat(counter.get(), is(1));
  }

}