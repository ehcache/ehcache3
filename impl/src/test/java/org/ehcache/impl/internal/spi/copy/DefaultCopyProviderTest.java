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

package org.ehcache.impl.internal.spi.copy;

import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.impl.copy.ReadWriteCopier;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.spi.copy.CopyProvider;
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
        (Class)TestCopier.class, DefaultCopierConfiguration.Type.KEY);

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
        (Class)SerializingCopier.class, DefaultCopierConfiguration.Type.KEY);

    assertThat(copyProvider.createKeyCopier(Long.class, mock(Serializer.class), config), instanceOf(SerializingCopier.class));
  }

  @Test
  public void testCreateValueCopierWithCustomCopierConfig() {
    DefaultCopyProvider provider = new DefaultCopyProvider(null);

    DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<Long>(
        (Class)TestCopier.class, DefaultCopierConfiguration.Type.VALUE);

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
        (Class)SerializingCopier.class, DefaultCopierConfiguration.Type.VALUE);

    assertThat(copyProvider.createValueCopier(Long.class, mock(Serializer.class), config), instanceOf(SerializingCopier.class));
  }

  @Test
  public void testCreateKeyValueCopierForImmutableTypes() {
    DefaultCopyProvider provider = new DefaultCopyProvider(null);

    assertIdentityCopierFor(provider, Long.class);
    assertIdentityCopierFor(provider, String.class);
    assertIdentityCopierFor(provider, Character.class);
    assertIdentityCopierFor(provider, Float.class);
    assertIdentityCopierFor(provider, Double.class);
    assertIdentityCopierFor(provider, Integer.class);
  }

  @Test
  public void testKeyValueCopierForImmutableTypesIsOverriden() {
    DefaultCopyProviderConfiguration configuration = new DefaultCopyProviderConfiguration();
    configuration.addCopierFor(Long.class, (Class)TestCopier.class);
    configuration.addCopierFor(String.class, (Class)TestCopier.class);
    configuration.addCopierFor(Character.class, (Class)TestCopier.class);
    configuration.addCopierFor(Float.class, (Class)TestCopier.class);
    configuration.addCopierFor(Double.class, (Class)TestCopier.class);
    configuration.addCopierFor(Integer.class, (Class)TestCopier.class);
    DefaultCopyProvider provider = new DefaultCopyProvider(configuration);

    assertIdentityCopierFor(provider, Long.class);
    assertIdentityCopierFor(provider, String.class);
    assertIdentityCopierFor(provider, Character.class);
    assertIdentityCopierFor(provider, Float.class);
    assertIdentityCopierFor(provider, Double.class);
    assertIdentityCopierFor(provider, Integer.class);
  }


  private static void assertIdentityCopierFor(CopyProvider provider, Class clazz) {
    assertThat(provider.createValueCopier(clazz, null), instanceOf(IdentityCopier.class));
    assertThat(provider.createKeyCopier(clazz, null), instanceOf(IdentityCopier.class));
  }

  public static class TestCopier<T> extends ReadWriteCopier<T> {

    @Override
    public T copy(final T obj) {
      return obj;
    }
  }
}