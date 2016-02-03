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

import org.ehcache.config.CacheManagerConfiguration;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class CacheManagerBuilderTest {

  @Test
  public void testIsExtensible() {

    final AtomicInteger counter = new AtomicInteger(0);

    final PersistentCacheManager cacheManager = newCacheManagerBuilder().with(new CacheManagerConfiguration<PersistentCacheManager>() {
      @SuppressWarnings("unchecked")
      @Override
      public CacheManagerBuilder<PersistentCacheManager> builder(final CacheManagerBuilder<? extends CacheManager> other) {
        counter.getAndIncrement();
        return mock(CacheManagerBuilder.class);
      }
    }).build(true);

    assertThat(cacheManager, nullValue());
    assertThat(counter.get(), is(1));
  }

}
