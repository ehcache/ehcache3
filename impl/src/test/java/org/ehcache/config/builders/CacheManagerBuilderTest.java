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

package org.ehcache.config.builders;

import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.impl.serialization.CompactJavaSerializer;
import org.ehcache.impl.serialization.JavaSerializer;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertNotNull;
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

  @Test
  public void testCanOverrideCopierInConfig() {
    CacheManagerBuilder<CacheManager> managerBuilder = newCacheManagerBuilder()
        .withCopier(Long.class, (Class) IdentityCopier.class);
    assertNotNull(managerBuilder.withCopier(Long.class, (Class) SerializingCopier.class));
  }

  @Test
  public void testCanOverrideSerializerConfig() {
    CacheManagerBuilder managerBuilder = newCacheManagerBuilder()
        .withSerializer(String.class, (Class) JavaSerializer.class);
    assertNotNull(managerBuilder.withSerializer(String.class, (Class) CompactJavaSerializer.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicateServiceCreationConfigurationFails() {
    newCacheManagerBuilder().using(new DefaultCopyProviderConfiguration())
        .using(new DefaultCopyProviderConfiguration());
  }

  @Test
  public void testDuplicateServiceCreationConfigurationOkWhenExplicit() {
    assertNotNull(newCacheManagerBuilder().using(new DefaultCopyProviderConfiguration())
        .replacing(new DefaultCopyProviderConfiguration()));
  }

}
