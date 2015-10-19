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

/**
 * @author rism
 */

import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.SerializerConfiguration;
import org.ehcache.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.internal.TimeSourceConfiguration;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.FileBasedPersistenceContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertNotNull;

/**
 * @author rism
 */
public class CacheManagerInitializationTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @SuppressWarnings("unchecked")
  @Test
  public void testCacheManagerInitialization() throws Exception {
    TestTimeSource testTimeSource = new TestTimeSource();
    CacheConfigurationBuilder configurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .disk(10, MemoryUnit.MB))
        .add(new DefaultSerializerConfiguration<CharSequence>(TestSerializer.getBuggySerializer(true), SerializerConfiguration.Type.VALUE));
    CacheConfiguration<Integer, CharSequence> configuration = configurationBuilder.buildConfig(Integer.class, CharSequence.class);
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("myCache", configuration)
        .using(new TimeSourceConfiguration(testTimeSource))
        .with(new CacheManagerPersistenceConfiguration(folder.newFolder("tempData")))
        .build(false);
    StateTransitionException stateTransitionException = null;
    try {
      cacheManager.init();
    } catch (StateTransitionException ste) {
      // expected
      stateTransitionException = ste;
    }
    assertNotNull(stateTransitionException);
    CacheConfigurationBuilder secondConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .disk(10, MemoryUnit.MB))
        .add(new DefaultSerializerConfiguration<CharSequence>(TestSerializer.getBuggySerializer(false), SerializerConfiguration.Type.VALUE));
    CacheConfiguration<Integer, CharSequence> secondConfiguration = secondConfigurationBuilder.buildConfig(Integer.class, CharSequence.class);

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("myCache", secondConfiguration)
        .using(new TimeSourceConfiguration(testTimeSource))
        .with(new CacheManagerPersistenceConfiguration(folder.newFolder("tempData2")))
        .build(false);
    cacheManager.init();
  }

  private static class TestSerializer {
    public static Class<? extends Serializer<CharSequence>> getBuggySerializer(boolean buggy) {
      if (buggy) {
        return BuggySerializer.class;
      }
      return WorkingSerializer.class;
    }

    public static class BuggySerializer implements Serializer<CharSequence> {

      public BuggySerializer(ClassLoader classLoader) {
      }

      @Override
      public ByteBuffer serialize(CharSequence object) {
        return null;
      }

      @Override
      public CharSequence read(ByteBuffer binary) throws ClassNotFoundException {
        return null;
      }

      @Override
      public boolean equals(CharSequence object, ByteBuffer binary) throws ClassNotFoundException {
        return true;
      }

      @Override
      public void close() {
        //nothing
      }
    }

    public static class WorkingSerializer implements Serializer<CharSequence> {

      public WorkingSerializer(ClassLoader classLoader) {
      }

      public WorkingSerializer(ClassLoader classLoader, FileBasedPersistenceContext fileBasedPersistenceContext) {}

      @Override
      public ByteBuffer serialize(CharSequence object) {
        return null;
      }

      @Override
      public CharSequence read(ByteBuffer binary) throws ClassNotFoundException {
        return null;
      }

      @Override
      public boolean equals(CharSequence object, ByteBuffer binary) throws ClassNotFoundException {
        return true;
      }

      @Override
      public void close() {
        //nothing
      }
    }
  }
}
