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

package org.ehcache.impl.spi.copy;

import org.ehcache.config.copy.CopierConfiguration;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.internal.copy.IdentityCopier;
import org.ehcache.impl.internal.copy.ReadWriteCopier;
import org.ehcache.impl.internal.copy.SerializingCopier;
import org.ehcache.impl.spi.copy.DefaultCopyProvider;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Created by alsu on 20/08/15.
 */
public class DefaultCopyProviderTest {

  @Test
  public void testCreateKeyCopierWithCustomCopierConfig() {
    DefaultCopyProvider provider = new DefaultCopyProvider(null);

    DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<Long>(
        (Class)TestCopier.class, CopierConfiguration.Type.KEY);

    assertThat(provider.createKeyCopier(Long.class, null, config), instanceOf(TestCopier.class));
  }

  @Test
  public void testCreateKeyCopierWithoutConfig() {
    DefaultCopyProvider provider = new DefaultCopyProvider(null);

    assertThat(provider.createKeyCopier(Long.class, null), instanceOf(IdentityCopier.class));
  }

  @Test
  public void testCreateKeyCopierWithSerializer() {
    DefaultCopyProvider copyProvider = new DefaultCopyProvider(null);
    DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<Long>(
        (Class)SerializingCopier.class, CopierConfiguration.Type.KEY);

    assertThat(copyProvider.createKeyCopier(Long.class, mock(Serializer.class), config), instanceOf(SerializingCopier.class));
  }

  @Test
  public void testCreateValueCopierWithCustomCopierConfig() {
    DefaultCopyProvider provider = new DefaultCopyProvider(null);

    DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<Long>(
        (Class)TestCopier.class, CopierConfiguration.Type.VALUE);

    assertThat(provider.createValueCopier(Long.class, null, config), instanceOf(TestCopier.class));
  }

  @Test
  public void testCreateValueCopierWithoutConfig() {
    DefaultCopyProvider provider = new DefaultCopyProvider(null);

    assertThat(provider.createValueCopier(Long.class, null), instanceOf(IdentityCopier.class));
  }

  @Test
  public void testCreateValueCopierWithSerializer() {
    DefaultCopyProvider copyProvider = new DefaultCopyProvider(null);
    DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<Long>(
        (Class)SerializingCopier.class, CopierConfiguration.Type.VALUE);

    assertThat(copyProvider.createValueCopier(Long.class, mock(Serializer.class), config), instanceOf(SerializingCopier.class));
  }

  public static class TestCopier<T> extends ReadWriteCopier<T> {

    @Override
    public T copy(final T obj) {
      return obj;
    }
  }
}