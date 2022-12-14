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

import org.ehcache.UserManagedCache;
import org.ehcache.config.builders.UserManagedCacheBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UserManagedCacheLoaderWriterTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testLoaderWriterWithUserManagedCache() throws Exception {
    CacheLoaderWriter<Long, Long> cacheLoaderWriter = mock(CacheLoaderWriter.class);

    UserManagedCache<Long, Long> userManagedCache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, Long.class)
            .withResourcePools(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
            .withLoaderWriter(cacheLoaderWriter).build(true);

    userManagedCache.put(1L, 1L);
    verify(cacheLoaderWriter, times(1)).write(eq(1L), eq(1L));

    when(cacheLoaderWriter.load(anyLong())).thenReturn(2L);
    Assert.assertThat(userManagedCache.get(2L), Matchers.is(2L));
  }
}
